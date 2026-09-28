// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// authorityCA returns a CA's PEM and a TLS certificate it issued for 127.0.0.1.
func authorityCA(t *testing.T) ([]byte, tls.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "authority CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "authority"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
}

// privateBundle writes data where the test user owns every directory and no
// one else can write; t.TempDir cannot serve on Linux or macOS.
func privateBundle(t *testing.T, data []byte) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(wd, ".trust-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if dir, err = filepath.EvalSymlinks(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "authority-ca.pem")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func authorityTrustConfig(t *testing.T, bundle string) config.Config {
	t.Helper()
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: filepath.Join(t.TempDir(), "outbound"),
		ReturnQueueDirectory: filepath.Join(t.TempDir(), "returned"), MaxItems: 10,
		MaxBytes: 8 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
		AuthorityCAFile: bundle,
	}
	return cfg
}

// TestTheConfiguredAuthorityBundleReachesDelivery drives the gateway's real
// construction: an artifact handed off is delivered over TLS to an authority
// certified only by the configured CA, which no system trust store holds.
func TestTheConfiguredAuthorityBundleReachesDelivery(t *testing.T) {
	caPEM, leaf := authorityCA(t)
	var delivered atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/evidence/artifacts" {
			delivered.Add(1)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{leaf}}
	server.StartTLS()
	defer server.Close()
	t.Setenv("EVIDENCE_ENDPOINT", server.URL+"/v1/evidence/artifacts")

	cfg := authorityTrustConfig(t, privateBundle(t, caPEM))
	mainBroker, evidenceBroker := newFakeBroker(), newFakeBroker()
	deps := baseDeps(t, cfg, mainBroker, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.authorityTrustOwner = os.Getuid()
	deps.newBroker = func(opts broker.Options) (brokerClient, error) {
		if opts.ClientID == defaultEvidenceClientID {
			return evidenceBroker, nil
		}
		return mainBroker, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(2 * time.Second)
	for evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter) == nil {
		if time.Now().After(deadline) {
			t.Fatal("outbound evidence subscription never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	artifact := `{"v":1,"device_id":"dev-01","high_water_seq":1,"signature":"ed25519:opaque"}`
	raw, _ := json.Marshal(map[string]string{
		"device_id": "dev-01", "artifact_type": "checkpoint",
		"artifact_b64": base64.StdEncoding.EncodeToString([]byte(artifact)),
	})
	evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter)("ori/dev-01/evidence/outbound", raw)

	deadline = time.Now().Add(3 * time.Second)
	for delivered.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no delivery reached the authority certified by the configured CA")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAnUnusableAuthorityBundlePreventsStartup(t *testing.T) {
	caPEM, _ := authorityCA(t)
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	for _, tc := range []struct {
		name  string
		data  []byte
		owner func() int
		want  string
	}{
		// The gateway's own dependencies trust root alone, so a bundle the
		// test user owns is refused, as an operator-owned file would be.
		{"owned by a user other than root", caPEM, func() int { return defaultDependencies().authorityTrustOwner }, "owned by an untrusted user"},
		{"not a CA bundle", []byte("not a certificate"), os.Getuid, "not a PEM certificate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if os.Getuid() == 0 && strings.Contains(tc.want, "untrusted") {
				t.Skip("root is always a trusted owner")
			}
			cfg := authorityTrustConfig(t, privateBundle(t, tc.data))
			fb := newFakeBroker()
			deps := baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
			deps.authorityTrustOwner = tc.owner()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := runGateway(ctx, "gateway.yaml", deps)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the gateway started with an unusable authority bundle: %v", err)
			}
		})
	}
}
