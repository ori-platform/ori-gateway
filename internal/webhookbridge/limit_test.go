// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package webhookbridge

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
)

// forwarded records what reached the runtime: each body and whether its
// signature verifies under the bridge's secret.
type forwarded struct {
	mu     sync.Mutex
	bodies []string
	bad    int
}

func (f *forwarded) handler(w http.ResponseWriter, r *http.Request) {
	payload, _ := io.ReadAll(r.Body)
	var signedAt int64
	_, _ = fmt.Sscan(r.Header.Get("X-Ori-Webhook-Timestamp"), &signedAt)
	f.mu.Lock()
	f.bodies = append(f.bodies, string(payload))
	if r.Header.Get("X-Ori-Webhook-Signature") != signBody(payload, "bridge-secret", signedAt, r.Header.Get("X-Ori-Webhook-Nonce")) {
		f.bad++
	}
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *forwarded) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.bodies)
}

type lockedWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func (l *lockedWriter) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.String()
}

func sumRefused(logs string) int {
	total := 0
	for line := range strings.SplitSeq(logs, "\n") {
		if _, rest, ok := strings.Cut(line, "refused="); ok {
			var n int
			_, _ = fmt.Sscan(rest, &n)
			total += n
		}
	}
	return total
}

type limitRig struct {
	bridge  *Server
	runtime *forwarded
	logs    *lockedWriter
	clockMu sync.Mutex
	clock   time.Time
}

func newLimitRig(t *testing.T, mutate func(*config.WebhookBridgeConfig)) *limitRig {
	t.Helper()
	r := &limitRig{runtime: &forwarded{}, logs: &lockedWriter{w: &bytes.Buffer{}}, clock: time.UnixMilli(1_700_000_000_123)}
	target := httptest.NewServer(http.HandlerFunc(r.runtime.handler))
	t.Cleanup(target.Close)
	cfg := testConfig(target.URL)
	cfg.MaxBodyBytes = 256
	if mutate != nil {
		mutate(&cfg)
	}
	bridge, err := New(cfg, Options{
		Now:   r.now,
		Nonce: func() (string, error) { return "nonce-1", nil },
		Getenv: func(name string) string {
			return map[string]string{"RUNTIME_TOKEN": "runtime-token", "WEBHOOK_HMAC_SECRET": "bridge-secret"}[name]
		},
		Logger:   slog.New(slog.NewTextHandler(r.logs, nil)),
		Refusals: NewRefusals(),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.bridge = bridge
	return r
}

func (r *limitRig) now() time.Time {
	r.clockMu.Lock()
	defer r.clockMu.Unlock()
	return r.clock
}

func (r *limitRig) advance(d time.Duration) {
	r.clockMu.Lock()
	r.clock = r.clock.Add(d)
	r.clockMu.Unlock()
}

// send posts body through the bridge's real handler and returns the status.
func (r *limitRig) send(body string) int {
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:55123"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.bridge.ServeHTTP(rr, req)
	return rr.Code
}

func reply(sender, text string) string {
	return "from=" + strings.ReplaceAll(sender, "+", "%2B") + "&text=" + text
}

// TestOneSendersBurstIsRefusedWhileAnotherSenderIsForwarded sends one
// sender's burst and one more, then another sender's reply in the same
// instant: the extra request is refused with 429 and never reaches the
// runtime, the other sender's is forwarded, and the refused sender is
// admitted again once its bucket refills.
func TestOneSendersBurstIsRefusedWhileAnotherSenderIsForwarded(t *testing.T) {
	r := newLimitRig(t, nil)
	for i := range config.DefaultWebhookBridgeSenderBurst {
		if code := r.send(reply("+2348000000001", fmt.Sprintf("YES-%d", i))); code != http.StatusOK {
			t.Fatalf("request %d within the sender's burst = %d", i, code)
		}
	}
	if code := r.send(reply("+2348000000001", "YES-extra")); code != http.StatusTooManyRequests {
		t.Fatalf("request past the sender's burst = %d, want 429", code)
	}
	if code := r.send(reply("+2348000000002", "YES-other")); code != http.StatusOK {
		t.Fatalf("another sender in the same window = %d, want forwarded", code)
	}
	r.advance(time.Minute / config.DefaultWebhookBridgeSenderPerMinute)
	if code := r.send(reply("+2348000000001", "YES-later")); code != http.StatusOK {
		t.Fatalf("the sender after its bucket refilled = %d", code)
	}
	if r.runtime.count() != config.DefaultWebhookBridgeSenderBurst+2 || r.bridge.refusals.Sender() != 1 {
		t.Fatalf("%d forwarded, %d refused by sender; want %d and 1",
			r.runtime.count(), r.bridge.refusals.Sender(), config.DefaultWebhookBridgeSenderBurst+2)
	}
	for _, body := range r.runtime.bodies {
		if strings.Contains(body, "YES-extra") {
			t.Fatal("a refused request reached the runtime")
		}
	}
}

// sendTyped posts body with a content type through the bridge's real handler.
func (r *limitRig) sendTyped(contentType, body string) int {
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sms/africastalking", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:55123"
	req.Header.Set("Content-Type", contentType)
	rr := httptest.NewRecorder()
	r.bridge.ServeHTTP(rr, req)
	return rr.Code
}

// TestEverySpellingOfOneNumberIsOneSender keys the limiter on the sender as
// the runtime reads it: digits and `+` only, from a form or a JSON body, with
// `from_number` when `from` is empty, and despite a malformed pair elsewhere
// in the form. Once the number's burst is spent, every spelling is refused.
func TestEverySpellingOfOneNumberIsOneSender(t *testing.T) {
	r := newLimitRig(t, func(cfg *config.WebhookBridgeConfig) { cfg.MaxBodyBytes = 512 })
	for i := range config.DefaultWebhookBridgeSenderBurst {
		if code := r.send(reply("+254700000001", fmt.Sprintf("YES-%d", i))); code != http.StatusOK {
			t.Fatalf("request %d within the burst = %d", i, code)
		}
	}
	form := "application/x-www-form-urlencoded"
	for _, c := range []struct{ contentType, body string }{
		{form, "from=%2B254-700-000-001&text=x"},
		{form, "from=%2B254+700+000+001&text=x"},
		{form, "from=%2B254700000001--&text=x"},
		{form, "from=&from_number=%2B254700000001&text=x"},
		{form, "from=%2B254700000001&text=yes;1"},
		{"application/json", `{"from":"+254700000001","text":"x","p":"&from=+111"}`},
		{"application/json; charset=utf-8", `{"from":"","from_number":"+254 700 000 001","text":"x"}`},
	} {
		if code := r.sendTyped(c.contentType, c.body); code != http.StatusTooManyRequests {
			t.Fatalf("%s %q = %d, want refused as the same sender", c.contentType, c.body, code)
		}
	}
}

// TestDeliveryReportsDrawOnTheCeilingAlone sends delivery reports, which name
// no sender, past a sender's burst: each is admitted up to the bridge-wide
// ceiling, and only the ceiling refuses one.
func TestDeliveryReportsDrawOnTheCeilingAlone(t *testing.T) {
	r := newLimitRig(t, nil)
	for i := range config.DefaultWebhookBridgeGlobalBurst {
		if code := r.send(fmt.Sprintf("id=ATX-%d&status=Success&phoneNumber=%%2B2547%08d", i, i)); code != http.StatusOK {
			t.Fatalf("delivery report %d within the ceiling = %d", i, code)
		}
	}
	if code := r.send("id=ATX-extra&status=Success"); code != http.StatusTooManyRequests {
		t.Fatalf("a delivery report past the ceiling = %d, want 429", code)
	}
	if r.bridge.refusals.Sender() != 0 || r.bridge.refusals.Global() != 1 {
		t.Fatalf("refusals sender=%d global=%d, want 0 and 1", r.bridge.refusals.Sender(), r.bridge.refusals.Global())
	}
}

// TestAFloodNeverSpendsAnOperatorsOwnAllowance fills the ceiling with
// distinct senders, then lets an operator try during the flood: refused by
// the ceiling, the operator keeps every token, so once the ceiling refills
// the operator's whole burst is admitted.
func TestAFloodNeverSpendsAnOperatorsOwnAllowance(t *testing.T) {
	r := newLimitRig(t, nil)
	for i := range config.DefaultWebhookBridgeGlobalBurst {
		r.send(reply(fmt.Sprintf("+234900%07d", i), "FLOOD"))
	}
	for range config.DefaultWebhookBridgeSenderBurst {
		if code := r.send(reply("+2348000000001", "YES")); code != http.StatusTooManyRequests {
			t.Fatalf("the operator during the flood = %d, want refused by the ceiling", code)
		}
	}
	// Ten seconds refills the ceiling's burst but not the operator's own
	// bucket, which only the refund keeps full.
	r.advance(10 * time.Second)
	for i := range config.DefaultWebhookBridgeSenderBurst {
		if code := r.send(reply("+2348000000001", "YES")); code != http.StatusOK {
			t.Fatalf("the operator's request %d after the flood = %d, want its own burst intact", i, code)
		}
	}
}

// TestEveryRefusalIsLoggedEventually refuses a burst inside one pacing
// interval and then stops: the refusals after the first line are flushed
// when the interval ends.
func TestEveryRefusalIsLoggedEventually(t *testing.T) {
	r := newLimitRig(t, nil)
	logs := r.logs
	for range config.DefaultWebhookBridgeSenderBurst + 30 {
		r.send(reply("+2348000000001", "YES"))
	}
	deadline := time.Now().Add(5 * time.Second)
	for sumRefused(logs.String()) != 30 {
		if time.Now().After(deadline) {
			t.Fatalf("logged %d refusals, want 30:\n%s", sumRefused(logs.String()), logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDistinctSendersAreHeldByTheGlobalCeilingInBoundedMemory sends a stream
// of distinct senders far past the tracked cap in one instant: the
// bridge-wide ceiling forwards only its burst, and the sender state never
// exceeds its cap.
func TestDistinctSendersAreHeldByTheGlobalCeilingInBoundedMemory(t *testing.T) {
	const maxSenders = 64
	r := newLimitRig(t, func(cfg *config.WebhookBridgeConfig) { cfg.MaxSenders = maxSenders })
	for i := range 10 * maxSenders {
		r.send(reply(fmt.Sprintf("+234900%07d", i), "HELLO"))
		if tracked := r.bridge.limiter.tracked(); tracked > maxSenders {
			t.Fatalf("%d senders tracked, cap %d", tracked, maxSenders)
		}
	}
	if r.runtime.count() != config.DefaultWebhookBridgeGlobalBurst {
		t.Fatalf("%d forwarded, want the ceiling's burst of %d", r.runtime.count(), config.DefaultWebhookBridgeGlobalBurst)
	}
	if want := uint64(10*maxSenders - config.DefaultWebhookBridgeGlobalBurst); r.bridge.refusals.Global() != want {
		t.Fatalf("%d refused by the ceiling, want %d", r.bridge.refusals.Global(), want)
	}
}

// TestForwardedBodiesStayExactAndSignedUnderLimiting forwards a mix of
// admitted and refused requests: every forwarded body is the provider's bytes
// exactly and its signature verifies.
func TestForwardedBodiesStayExactAndSignedUnderLimiting(t *testing.T) {
	r := newLimitRig(t, nil)
	sent := map[string]bool{}
	for i := range 3 * config.DefaultWebhookBridgeSenderBurst {
		body := reply(fmt.Sprintf("+23480000000%02d", i%3), fmt.Sprintf("YES-%02d%%20ok", i))
		if r.send(body) == http.StatusOK {
			sent[body] = true
		}
	}
	if r.runtime.bad != 0 {
		t.Fatalf("%d forwarded signatures did not verify", r.runtime.bad)
	}
	for _, body := range r.runtime.bodies {
		if !sent[body] {
			t.Fatalf("forwarded %q, which the provider did not send or the bridge refused", body)
		}
	}
	if len(r.runtime.bodies) != len(sent) {
		t.Fatalf("%d forwarded, %d admitted", len(r.runtime.bodies), len(sent))
	}
}

// TestRefusalsLogNoSenderHashOrContent floods one sender and many senders:
// the log names only the limit and a count.
func TestRefusalsLogNoSenderHashOrContent(t *testing.T) {
	r := newLimitRig(t, nil)
	for range 20 {
		r.send(reply("+2348000000001", "SECRET-TEXT"))
	}
	for i := range 40 {
		r.send(reply(fmt.Sprintf("+234900%07d", i), "SECRET-TEXT"))
	}
	logs := r.logs.String()
	if !strings.Contains(logs, "limit=sender") || !strings.Contains(logs, "limit=global") {
		t.Fatalf("refusals were not logged:\n%s", logs)
	}
	key := r.bridge.limiter.keyOf("+2348000000001")
	for _, leak := range []string{"2348000000001", "234900", "SECRET-TEXT", fmt.Sprintf("%x", key[:8]), "runtime-token", "bridge-secret"} {
		if strings.Contains(logs, leak) {
			t.Fatalf("the log carries %q:\n%s", leak, logs)
		}
	}
}

// TestSendersTheBridgeCannotReadShareOneLimitedBucket sends bodies whose
// sender the runtime may read but the bridge cannot follow the same way: a
// JSON `from` that is a number, a list, an object or beside a NaN, and form
// pairs Go cannot decode. Each fails closed to one shared per-sender bucket,
// so none escapes a per-sender limit by drawing on the ceiling alone.
func TestSendersTheBridgeCannotReadShareOneLimitedBucket(t *testing.T) {
	r := newLimitRig(t, func(cfg *config.WebhookBridgeConfig) { cfg.MaxBodyBytes = 512 })
	const jsonType = "application/json"
	const form = "application/x-www-form-urlencoded"
	unreadable := []struct{ contentType, body string }{
		{jsonType, `{"from":["+254712345678"],"text":"x"}`},
		{jsonType, `{"from":{"a":"+254712345678"},"text":"x"}`},
		{jsonType, `{"from":"+254712345678","x":NaN}`},
		{jsonType, `{"from":0,"from_number":"+254712345678"}`},
		{jsonType, `{"from":0e7,"from_number":"+254712345678"}`},
		{jsonType, `{"from":2.54712345678e11}`},
		{form, "from=%2B254712345678;&text=x"},
		{form, "from=%2B254712345678%zz&text=x"},
		{form, "from=&text=x"},
	}
	for i, c := range unreadable {
		want := http.StatusOK
		if i >= config.DefaultWebhookBridgeSenderBurst {
			want = http.StatusTooManyRequests
		}
		if code := r.sendTyped(c.contentType, c.body); code != want {
			t.Fatalf("%s %q = %d, want %d: unreadable senders share one bucket", c.contentType, c.body, code, want)
		}
	}
	if r.bridge.refusals.Global() != 0 {
		t.Fatal("an unreadable sender drew on the ceiling alone")
	}
}
