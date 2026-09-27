// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build evidence_integration

// This test drives the real courier against a running evidence authority
// ingest service. It is not part of the default test run. To run it, build the
// service and point the test at the binary:
//
//	ORI_EVIDENCE_INGEST_BIN=/path/to/ori-evidence-ingest \
//	  go test -tags evidence_integration -run TestAgainstTheEvidenceAuthority -v ./internal/evidence
//
// The signed registration is the vendored ori-specs vector. The service binds loopback HTTP behind TLS
// termination, so the test fronts it with a TLS proxy. The proxy can rewrite
// the path, the method, or drop the authentication header, to reach the
// service's framework and transport refusals; it never alters a response.

package courier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	canonicaljson "github.com/ori-platform/ori-canonicaljson"
)

type proxyMode struct {
	path       string
	method     string
	dropAuth   bool
	lastStatus int
	lastType   string
	lastBody   string
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func vectorCanonicalBytes(t *testing.T, relative string) []byte {
	t.Helper()
	raw, err := specvectors.Read(relative)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []struct {
			Name     string          `json:"name"`
			Artifact json.RawMessage `json:"artifact"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Cases {
		if c.Name == "valid" {
			// The wire artifact is the canonical form of the signed object.
			var value map[string]any
			decoder := json.NewDecoder(bytes.NewReader(c.Artifact))
			decoder.UseNumber()
			if err := decoder.Decode(&value); err != nil {
				t.Fatal(err)
			}
			out, err := canonicaljson.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			return out
		}
	}
	t.Fatalf("%s has no valid case", relative)
	return nil
}

func TestAgainstTheEvidenceAuthority(t *testing.T) {
	bin := os.Getenv("ORI_EVIDENCE_INGEST_BIN")
	if bin == "" {
		t.Fatal("ORI_EVIDENCE_INGEST_BIN must name a built evidence ingest service binary")
	}
	registration := vectorCanonicalBytes(t, "evidence-exchange/vectors/anchor-registration-v2.json")
	device := recordDevice(registration)
	const secret = "evidence-ingest-secret-with-at-least-32-bytes-integration"
	const clientID = "site-a-gateway"

	work := t.TempDir()
	bind := freeLoopbackAddr(t)
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"ORI_EVIDENCE_POSTURE=development",
		"ORI_EVIDENCE_TLS_TERMINATED=true",
		"ORI_EVIDENCE_BIND="+bind,
		"ORI_EVIDENCE_KEY_SEALING_SECRET=integration-sealing-secret-with-enough-bytes",
		"ORI_EVIDENCE_EPOCH_KEY_PATH="+filepath.Join(work, "epoch.key"),
		"ORI_EVIDENCE_RECEIPT_KEY_PATH="+filepath.Join(work, "receipt.key"),
		"ORI_EVIDENCE_COMMISSIONING_KEY_ID=commissioning-integration",
		"ORI_EVIDENCE_COMMISSIONING_PUBLIC_KEY_HEX="+strings.Repeat("11", 32),
		"ORI_EVIDENCE_ALLOWED_DEVICES="+device,
		"ORI_EVIDENCE_CLIENT_ID="+clientID,
		"ORI_EVIDENCE_INGEST_SECRET="+secret,
		"ORI_EVIDENCE_DATABASE_PATH="+filepath.Join(work, "authority.db"),
		"ORI_EVIDENCE_EPOCH_KEY_ID=epoch-integration",
		"ORI_EVIDENCE_RECEIPT_KEY_ID=receipt-integration",
		"ORI_EVIDENCE_CHAIN_SIGNING_PUBLIC_KEY_HEX="+strings.Repeat("22", 32),
	)
	var serviceLog bytes.Buffer
	cmd.Stdout, cmd.Stderr = &serviceLog, &serviceLog
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := net.Dial("tcp", bind)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("service did not start: %s", serviceLog.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	var modeMu sync.Mutex
	mode := &proxyMode{}
	proxy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modeMu.Lock()
		m := *mode
		modeMu.Unlock()
		body, _ := io.ReadAll(r.Body)
		path, method := r.URL.Path, r.Method
		if m.path != "" {
			path = m.path
		}
		if m.method != "" {
			method = m.method
		}
		req, _ := http.NewRequest(method, "http://"+bind+path, bytes.NewReader(body))
		for k, v := range r.Header {
			if m.dropAuth && strings.EqualFold(k, "Authorization") {
				continue
			}
			req.Header[k] = v
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, "upstream unreachable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		modeMu.Lock()
		mode.lastStatus, mode.lastType, mode.lastBody = resp.StatusCode, resp.Header.Get("Content-Type"), string(respBody)
		modeMu.Unlock()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)
	}))
	t.Cleanup(proxy.Close)

	oversize := []byte(`{"v":1,"device_id":"` + device + `","high_water_seq":1,"pad":"` + strings.Repeat("x", maxArtifactBytes) + `"}`)
	v2 := []byte(`{"v":2,"device_id":"` + device + `","high_water_seq":1,"signature":"ed25519:opaque"}`)

	for _, tc := range []struct {
		name string
		mode proxyMode
		kind ArtifactType
		body []byte
		// Draft expectations: what a conforming service answers, and what
		// the courier must then do with whatever the service did answer.
		draftAnswer string
	}{
		{"clean acceptance", proxyMode{}, ArtifactAnchorRegistration, registration, "200 accepted"},
		{"missing authentication header", proxyMode{dropAuth: true}, ArtifactAnchorRegistration, registration, "401 malformed"},
		{"a v2 artifact", proxyMode{}, ArtifactCheckpoint, v2, "422 unrecognised_version"},
		{"404 from an unknown path", proxyMode{path: "/v1/evidence/unknown"}, ArtifactAnchorRegistration, registration, "any; unrecognised outcome"},
		{"405 from a wrong method", proxyMode{method: http.MethodPut}, ArtifactAnchorRegistration, registration, "any; unrecognised outcome"},
		{"413 from a body over 1 MiB", proxyMode{}, ArtifactCheckpoint, oversize, "413 from the body limit; unrecognised outcome"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			modeMu.Lock()
			*mode = tc.mode
			modeMu.Unlock()
			// Admission is the real courier's, except for the oversize body,
			// which the admission bound would refuse: that one is written to
			// the queue directly so the service's own body limit is reached.
			q := openTestQueue(t, 10, 1<<24)
			if len(tc.body) > maxArtifactBytes {
				if _, err := q.Enqueue(tc.kind, tc.body); err != nil {
					t.Fatal(err)
				}
			} else {
				courier, err := NewCourier(q, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := courier.Admit(tc.kind, tc.body); err != nil {
					t.Fatalf("admission refused: %v", err)
				}
			}
			channel, err := NewHTTPChannel(HTTPChannelOptions{
				Endpoint: proxy.URL + ingestPath, ClientID: clientID, Secret: secret, HTTPClient: proxy.Client(),
			})
			if err != nil {
				t.Fatal(err)
			}
			logs := &syncBuffer{}
			worker, err := NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
				RetryInterval: time.Hour, BlockedReminderInterval: time.Hour, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			running := startWorker(t, worker)
			waitFor(t, "an outcome", func() bool { return q.Len() == 0 || worker.Status().LastError != "" })
			running.stop()
			modeMu.Lock()
			seen := fmt.Sprintf("%d %s %s", mode.lastStatus, mode.lastType, strings.TrimSpace(mode.lastBody))
			modeMu.Unlock()
			class, reason := "retire", ""
			if q.Len() != 0 {
				entry, ok := laneEntry(worker, LaneKey{Device: recordDevice(tc.body), Lane: LaneOf(tc.kind)})
				if !ok {
					t.Fatalf("not retired and not reported")
				}
				class = entry.State
				if entry.Held != nil {
					reason = entry.Held.Reason
				} else if lines := logs.records(t, stallMessage); len(lines) == 1 {
					reason, _ = lines[0]["reason"].(string)
				}
			}
			t.Logf("service answered: %s", seen)
			t.Logf("draft answer: %s; courier: %s (reason %q)", tc.draftAnswer, class, reason)
			// The courier's side, for what the service actually sent.
			switch {
			case strings.HasPrefix(seen, "200 ") && strings.Contains(seen, `"accepted"`):
				if class != "retire" {
					t.Errorf("a clean acceptance was not retired: %s", class)
				}
			case strings.HasPrefix(seen, "401 ") && strings.Contains(seen, "application/json"):
				if class != DeviceWaitingHandoff {
					t.Errorf("a 401 was %s, want waiting_handoff", class)
				}
			case !strings.Contains(seen, "application/json"):
				if class != DeviceBackingOff || reason != "unrecognised" {
					t.Errorf("a non-JSON answer was %s (%q), want an unrecognised back-off", class, reason)
				}
			}
			if class == "retire" && q.Len() != 0 {
				t.Error("retired class with evidence still queued")
			}
		})
	}
}
