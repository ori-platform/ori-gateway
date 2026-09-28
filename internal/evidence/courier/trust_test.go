// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T, name string) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// leaf issues a server certificate for the given names from ca.
func (ca testCA) leaf(t *testing.T, dnsNames []string, ips []net.IP) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "authority"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// trustDir makes a private directory whose every ancestor the test user or
// root owns and no one else can write. t.TempDir cannot serve: /tmp is
// world-writable on Linux, and /var is a symlink on macOS.
func trustDir(t *testing.T) string {
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
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	return real
}

func writeBundleIn(t *testing.T, dir string, data []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, "authority-ca.pem")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeBundle(t *testing.T, data []byte, mode os.FileMode) string {
	t.Helper()
	return writeBundleIn(t, trustDir(t), data, mode)
}

// authorityServer serves the ingest path over TLS with cert, answering 204.
func authorityServer(t *testing.T, cert tls.Certificate) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

// reach makes one HTTPS request to the authority through a channel built
// with the bundle at path, and returns its error.
func reach(t *testing.T, path, url string) error {
	t.Helper()
	pool, err := LoadAuthorityTrust(path, os.Getuid())
	if err != nil {
		t.Fatal(err)
	}
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint:     url + ingestPath,
		ClientID:     "gateway-test-01",
		Secret:       strings.Repeat("s", 32),
		AuthorityCAs: pool,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := channel.client.Get(url + ingestPath)
	if err == nil {
		response.Body.Close()
	}
	return err
}

func TestTheChannelTrustsOnlyTheAuthorityBundle(t *testing.T) {
	loopback := []net.IP{net.ParseIP("127.0.0.1")}
	current := newTestCA(t, "authority CA current")
	next := newTestCA(t, "authority CA next")
	other := newTestCA(t, "some other CA")

	served := authorityServer(t, current.leaf(t, nil, loopback))
	if err := reach(t, writeBundle(t, current.pem, 0o644), served.URL); err != nil {
		t.Fatalf("the bundle's own CA was refused: %v", err)
	}
	if err := reach(t, writeBundle(t, other.pem, 0o644), served.URL); err == nil {
		t.Fatal("a server certified by a CA outside the bundle was trusted")
	}

	misnamed := authorityServer(t, current.leaf(t, []string{"authority.example"}, nil))
	err := reach(t, writeBundle(t, current.pem, 0o644), misnamed.URL)
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("a certificate for another name was accepted: %v", err)
	}

	// A rotation: the bundle holds the current and the next CA, and the
	// authority has moved to a certificate from the next one.
	rotated := authorityServer(t, next.leaf(t, nil, loopback))
	both := append(append([]byte{}, current.pem...), next.pem...)
	if err := reach(t, writeBundle(t, both, 0o644), rotated.URL); err != nil {
		t.Fatalf("a rotation bundle refused the next CA: %v", err)
	}
	if err := reach(t, writeBundle(t, current.pem, 0o644), rotated.URL); err == nil {
		t.Fatal("the next CA was trusted before it was in the bundle")
	}
}

func TestTheTestServersOwnCertificateIsRefusedWithABundle(t *testing.T) {
	// httptest's own server certificate is trusted only through
	// server.Client(); with a bundle, the channel must refuse it.
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	bundle := writeBundle(t, newTestCA(t, "unrelated").pem, 0o644)
	if err := reach(t, bundle, server.URL); err == nil {
		t.Fatal("a certificate outside the bundle was trusted")
	}
}

func TestAnUnsafeOrMalformedBundleIsRefused(t *testing.T) {
	ca := newTestCA(t, "authority CA")
	leafDER := ca.leaf(t, nil, []net.IP{net.ParseIP("127.0.0.1")}).Certificate[0]
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: []byte{1, 2, 3}})
	cases := []struct {
		name string
		path func() string
		want string
	}{
		{"missing", func() string { return filepath.Join(trustDir(t), "absent.pem") }, "no such file"},
		{"a directory", func() string { return trustDir(t) }, "regular file"},
		{"relative", func() string { return "authority-ca.pem" }, "absolute and clean"},
		{"unclean", func() string {
			dir := trustDir(t)
			writeBundleIn(t, dir, ca.pem, 0o644)
			return dir + "/./authority-ca.pem"
		}, "absolute and clean"},
		{"a group-writable parent", func() string {
			dir := trustDir(t)
			path := writeBundleIn(t, dir, ca.pem, 0o644)
			chmod(t, dir, 0o770)
			return path
		}, "writable by group or others"},
		{"a world-writable parent", func() string {
			dir := trustDir(t)
			path := writeBundleIn(t, dir, ca.pem, 0o644)
			chmod(t, dir, 0o707)
			return path
		}, "writable by group or others"},
		{"a world-writable grandparent", func() string {
			outer := trustDir(t)
			inner := filepath.Join(outer, "inner")
			mkdir(t, inner)
			path := writeBundleIn(t, inner, ca.pem, 0o644)
			chmod(t, outer, 0o777)
			return path
		}, "writable by group or others"},
		{"a symlinked parent", func() string {
			outer := trustDir(t)
			real := filepath.Join(outer, "real")
			mkdir(t, real)
			writeBundleIn(t, real, ca.pem, 0o644)
			link := filepath.Join(outer, "link")
			if err := os.Symlink(real, link); err != nil {
				t.Fatal(err)
			}
			return filepath.Join(link, "authority-ca.pem")
		}, "is a symlink"},
		{"a symlinked bundle", func() string {
			dir := trustDir(t)
			target := writeBundleIn(t, dir, ca.pem, 0o644)
			link := filepath.Join(dir, "link.pem")
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			return link
		}, "is a symlink"},
		{"group writable", func() string { return writeBundle(t, ca.pem, 0o664) }, "writable"},
		{"world writable", func() string { return writeBundle(t, ca.pem, 0o646) }, "writable"},
		{"empty", func() string { return writeBundle(t, nil, 0o644) }, "no certificate"},
		{"not PEM", func() string { return writeBundle(t, []byte("not a certificate"), 0o644) }, "not a PEM"},
		{"trailing data", func() string { return writeBundle(t, append(append([]byte{}, ca.pem...), []byte("junk")...), 0o644) }, "not a PEM"},
		{"a private key", func() string { return writeBundle(t, append(append([]byte{}, ca.pem...), keyPEM...), 0o644) }, "PRIVATE KEY"},
		{"a leaf, not a CA", func() string { return writeBundle(t, leafPEM, 0o644) }, "not a CA"},
		{"a corrupt certificate", func() string {
			return writeBundle(t, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{0x30, 0x03}}), 0o644)
		}, "certificate 1"},
		{"too large", func() string { return writeBundle(t, make([]byte, maxAuthorityTrustBytes+1), 0o644) }, "exceeds"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadAuthorityTrust(tc.path(), os.Getuid())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestABundleOwnedByAnotherUserIsRefused(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root owns the bundle, and root is always trusted")
	}
	path := writeBundle(t, newTestCA(t, "authority CA").pem, 0o644)
	// The test user owns the bundle and every directory it made; trusting
	// only some other user makes each of them an untrusted owner.
	_, err := LoadAuthorityTrust(path, os.Getuid()+1)
	if err == nil || !strings.Contains(err.Error(), "owned by an untrusted user") {
		t.Fatalf("a bundle owned by an untrusted user was accepted: %v", err)
	}
	if _, err := LoadAuthorityTrust(path, os.Getuid()); err != nil {
		t.Fatalf("the same bundle under its trusted owner was refused: %v", err)
	}
}

// TestADirectoryOwnedByAnotherUserIsRefused needs root to hand a directory to
// another user. CI runs it under sudo; as any other user it skips.
func TestADirectoryOwnedByAnotherUserIsRefused(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("needs root to create a directory another user owns")
	}
	const other = 54321
	// Under /, so every ancestor is root's, wherever the checkout lives.
	outer, err := os.MkdirTemp("/", ".ori-trust-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(outer) })
	if err := os.Chmod(outer, 0o700); err != nil {
		t.Fatal(err)
	}
	inner := filepath.Join(outer, "inner")
	mkdir(t, inner)
	path := writeBundleIn(t, inner, newTestCA(t, "authority CA").pem, 0o644)
	if _, err := LoadAuthorityTrust(path, 0); err != nil {
		t.Fatalf("a root-owned bundle under root-owned directories was refused: %v", err)
	}
	for _, owned := range []string{outer, inner} {
		if err := os.Chown(owned, other, other); err != nil {
			t.Fatal(err)
		}
		_, err := LoadAuthorityTrust(path, 0)
		if err == nil || !strings.Contains(err.Error(), owned+" is owned by an untrusted user") {
			t.Fatalf("a directory owned by uid %d was trusted: %v", other, err)
		}
		if err := os.Chown(owned, 0, 0); err != nil {
			t.Fatal(err)
		}
	}
}

func chmod(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	// Restore access so cleanup can remove the tree.
	t.Cleanup(func() { _ = os.Chmod(path, 0o700) })
}

func mkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

func TestAChannelTakesAClientOrABundleNotBoth(t *testing.T) {
	pool := x509.NewCertPool()
	_, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint:     "https://authority.example" + ingestPath,
		ClientID:     "gateway-test-01",
		Secret:       strings.Repeat("s", 32),
		HTTPClient:   &http.Client{},
		AuthorityCAs: pool,
	})
	if err == nil {
		t.Fatal("a channel accepted both an HTTP client and authority CAs")
	}
}
