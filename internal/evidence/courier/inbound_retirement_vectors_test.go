// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

type inboundRetirementCorpus struct {
	VectorSet        string   `json:"vector_set"`
	Contract         string   `json:"contract"`
	Vocabulary       []string `json:"vocabulary"`
	VocabularySource string   `json:"vocabulary_source"`
	Cases            []struct {
		Name            string `json:"name"`
		Note            string `json:"note"`
		Acknowledgement *struct {
			Outcome string `json:"outcome"`
			Reason  string `json:"reason"`
		} `json:"acknowledgement"`
		Retire     bool    `json:"retire"`
		Head       string  `json:"head"`
		Redelivery *string `json:"redelivery"`
		Clause     string  `json:"clause"`
	} `json:"cases"`
}

// loadInboundRetirementCorpus decodes the corpus strictly, so a member this
// test does not interpret fails it rather than being ignored.
func loadInboundRetirementCorpus(t *testing.T) inboundRetirementCorpus {
	t.Helper()
	raw, err := specvectors.Read("gateway-evidence-carriage/vectors/inbound-retirement-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus inboundRetirementCorpus
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&corpus); err != nil {
		t.Fatalf("the inbound retirement corpus has a member this test does not interpret: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatal("the inbound retirement corpus has trailing data")
	}
	if corpus.Contract != "gateway-evidence-carriage/v1" || len(corpus.Vocabulary) == 0 || len(corpus.Cases) == 0 {
		t.Fatalf("unexpected inbound retirement corpus: %s with %d reasons and %d cases", corpus.Contract, len(corpus.Vocabulary), len(corpus.Cases))
	}
	return corpus
}

const (
	// rigRetry is the publisher's retry; its timer fires only when the test
	// fires it.
	rigRetry  = time.Hour
	rigWait   = 10 * time.Second
	rigSecret = "runtime-gateway-envelope-secret"
	rigDevice = "site-a-edge-01"
	rigTopic  = "ori/site-a-edge-01/evidence/inbound/ack"
)

// returnRig runs the real return publisher loop over queued receipts. Its
// retry timer is the rig's: the loop arms it through the publisher's after
// seam, and the test fires the armed timer, after checking it was re-armed
// with the retry, and waits for each iteration to finish before observing
// what it published.
type returnRig struct {
	t          *testing.T
	sink       *DurableAuthoritySink
	publisher  *ReturnPublisher
	artifact   []byte
	headID     string
	first      []byte
	published  chan []byte
	iterations chan struct{}

	timerMu sync.Mutex
	timer   *rigTimer
	armed   chan struct{}

	clockMu sync.Mutex
	clock   time.Time

	gateMu sync.Mutex
	gate   chan struct{}
}

func newReturnRig(t *testing.T, extra ...[]byte) *returnRig {
	t.Helper()
	r := &returnRig{
		t: t, published: make(chan []byte, 128), iterations: make(chan struct{}, 128),
		armed: make(chan struct{}, 1), clock: time.UnixMilli(1787000000000),
	}
	sink, err := OpenDurableAuthoritySink(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.sink = sink
	r.artifact = validReceiptBytes(rigDevice, 1, 1)
	for _, payload := range append([][]byte{r.artifact}, extra...) {
		if err := sink.Store(context.Background(), AuthorityArtifact{
			Type: AuthorityDeliveryReceipt, DeviceID: rigDevice, Payload: payload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	head, ok := sink.queue.Peek()
	if !ok {
		t.Fatal("the receipt was not queued")
	}
	r.headID = head.ID
	publisher, err := NewReturnPublisher(sink, testAckClock(t, r.now),
		func(_ context.Context, _ string, _ byte, _ bool, payload []byte) error {
			r.published <- append([]byte(nil), payload...)
			return nil
		}, rigSecret, "", r.now, rigRetry)
	if err != nil {
		t.Fatal(err)
	}
	publisher.after = r.after
	publisher.afterIteration = func() {
		r.iterations <- struct{}{}
		r.gateMu.Lock()
		gate := r.gate
		r.gate = nil
		r.gateMu.Unlock()
		if gate != nil {
			<-gate
		}
	}
	r.publisher = publisher
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = publisher.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	select {
	case <-r.armed:
	case <-time.After(rigWait):
		t.Fatal("the return publisher loop never armed its timer")
	}
	r.fire(0)
	r.waitIteration()
	carried := r.drain()
	if len(carried) != 1 {
		t.Fatalf("the first iteration carried %d envelopes, want 1", len(carried))
	}
	r.first = carried[0]
	return r
}

type rigTimer struct {
	d     time.Duration
	c     chan time.Time
	fired bool
}

func (r *returnRig) after(d time.Duration) <-chan time.Time {
	timer := &rigTimer{d: d, c: make(chan time.Time, 1)}
	r.timerMu.Lock()
	r.timer = timer
	r.timerMu.Unlock()
	select {
	case r.armed <- struct{}{}:
	default:
	}
	return timer.c
}

// fire fires the timer the loop armed last, which must be unfired and armed
// for want.
func (r *returnRig) fire(want time.Duration) {
	r.t.Helper()
	r.timerMu.Lock()
	timer := r.timer
	if timer == nil || timer.fired || timer.d != want {
		r.timerMu.Unlock()
		if timer == nil || timer.fired {
			r.t.Fatal("the loop did not re-arm its retry timer")
		}
		r.t.Fatalf("the loop armed its retry timer for %v, want %v", timer.d, want)
	}
	timer.fired = true
	r.timerMu.Unlock()
	timer.c <- r.now()
}

func (r *returnRig) now() time.Time {
	r.clockMu.Lock()
	defer r.clockMu.Unlock()
	return r.clock
}

func (r *returnRig) advance() {
	r.clockMu.Lock()
	r.clock = r.clock.Add(rigRetry)
	r.clockMu.Unlock()
}

// waitIteration waits for the loop to finish one iteration. The deadline is a
// liveness bound only; no assertion depends on it.
func (r *returnRig) waitIteration() {
	r.t.Helper()
	select {
	case <-r.iterations:
	case <-time.After(rigWait):
		r.t.Fatal("the return publisher loop did not complete an iteration")
	}
}

// drain returns what completed iterations published; publishing is
// synchronous within an iteration.
func (r *returnRig) drain() [][]byte {
	var carried [][]byte
	for {
		select {
		case payload := <-r.published:
			carried = append(carried, payload)
		default:
			return carried
		}
	}
}

// interval lets one retry interval elapse and returns what the loop carried.
func (r *returnRig) interval() [][]byte {
	r.t.Helper()
	r.advance()
	r.fire(rigRetry)
	r.waitIteration()
	return r.drain()
}

// stateChange signals a receiver state change after an interval has elapsed
// and returns what the loop carried.
func (r *returnRig) stateChange() [][]byte {
	r.t.Helper()
	r.advance()
	r.publisher.ReceiverStateChanged(rigDevice)
	r.waitIteration()
	return r.drain()
}

// whilePaused runs fn while the loop is held inside a retry-interval
// iteration, so every wake fn sends collapses into the loop's one pending
// wake, then releases the loop and returns what that one wake carried. fn must
// wake the loop.
func (r *returnRig) whilePaused(fn func()) [][]byte {
	r.t.Helper()
	gate := make(chan struct{})
	r.gateMu.Lock()
	r.gate = gate
	r.gateMu.Unlock()
	released := false
	release := func() {
		if !released {
			released = true
			close(gate)
		}
	}
	// Released even when the test fails here, or the loop never stops.
	defer release()
	// The clock is not advanced, so the head last carried is not carried
	// again in the iteration the loop pauses in.
	r.fire(rigRetry)
	r.waitIteration()
	if carried := r.drain(); len(carried) != 0 {
		r.t.Fatalf("the iteration the loop paused in carried %d envelopes", len(carried))
	}
	fn()
	release()
	r.waitIteration()
	return r.drain()
}

// handle delivers an acknowledgement. A retirement wakes the loop, so the
// iteration it causes is awaited before the caller observes anything.
func (r *returnRig) handle(topic string, payload []byte) error {
	r.t.Helper()
	before := r.sink.Len()
	err := r.publisher.HandleAck(topic, payload)
	if r.sink.Len() < before {
		r.waitIteration()
	}
	return err
}

// decision signs an acknowledgement from device naming the queued receipt.
func (r *returnRig) decision(device, outcome, reason string, mutate func(*inboundAck)) []byte {
	r.t.Helper()
	digest := sha256.Sum256(r.artifact)
	ack := inboundAck{
		DeviceID: device, ArtifactType: string(AuthorityDeliveryReceipt),
		ArtifactDigest: "sha256:" + hex.EncodeToString(digest[:]),
		Outcome:        outcome, Reason: reason, AcknowledgedAtMS: r.now().UnixMilli(),
	}
	if mutate != nil {
		mutate(&ack)
	}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceInboundAckMessageType, device, "", r.now().UnixMilli(), rigSecret)
	if err != nil {
		r.t.Fatal(err)
	}
	ack.Auth = &auth
	payload, err := json.Marshal(ack)
	if err != nil {
		r.t.Fatal(err)
	}
	return payload
}

func (r *returnRig) headKept() {
	r.t.Helper()
	head, ok := r.sink.queue.Peek()
	if !ok || head.ID != r.headID {
		r.t.Fatalf("the head was not kept: %d queued", r.sink.Len())
	}
}

func (r *returnRig) heldID() string {
	r.publisher.mu.Lock()
	defer r.publisher.mu.Unlock()
	return r.publisher.heldID
}

// held requires that intervals elapse without the head being carried.
func (r *returnRig) held() {
	r.t.Helper()
	for range 3 {
		if carried := r.interval(); len(carried) != 0 {
			r.t.Fatalf("a held head was carried %d times on the retry interval", len(carried))
		}
	}
	r.headKept()
}

// carriedAgain requires exactly one later delivery of artifact in a newly
// signed envelope.
func (r *returnRig) carriedAgain(carried [][]byte, artifact []byte) {
	r.t.Helper()
	if len(carried) != 1 {
		r.t.Fatalf("carried %d envelopes, want 1", len(carried))
	}
	var a, b inboundEnvelope
	if err := json.Unmarshal(r.first, &a); err != nil {
		r.t.Fatal(err)
	}
	if err := json.Unmarshal(carried[0], &b); err != nil {
		r.t.Fatal(err)
	}
	if string(b.Artifact) != string(artifact) || a.Auth == nil || b.Auth == nil || b.Auth.SignedAtMS <= a.Auth.SignedAtMS {
		r.t.Fatal("the artifact was not carried in a newly signed envelope")
	}
}

// TestInboundRetirementCorpus drives every vendored case through the real
// acknowledgement handler and the publisher loop: whether the decision is
// accepted, whether the head retires, and how it is carried again.
func TestInboundRetirementCorpus(t *testing.T) {
	corpus := loadInboundRetirementCorpus(t)
	for reason := range inboundRefusalRetires {
		if !slices.Contains(corpus.Vocabulary, reason) {
			t.Errorf("the gateway accepts %q, which the receiver rejection vocabulary does not list", reason)
		}
	}
	covered := map[string]bool{}
	for _, c := range corpus.Cases {
		if c.Acknowledgement != nil {
			covered[c.Acknowledgement.Reason] = true
		}
	}
	for _, reason := range corpus.Vocabulary {
		if !covered[reason] {
			t.Errorf("the corpus vocabulary lists %q but no case decides it", reason)
		}
	}
	for _, c := range corpus.Cases {
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			if (c.Head == "retired") != c.Retire || (c.Head != "retired" && c.Head != "kept") {
				t.Fatalf("case head %q disagrees with retire %v", c.Head, c.Retire)
			}
			rig := newReturnRig(t)
			if c.Acknowledgement == nil {
				if c.Retire || c.Redelivery == nil || *c.Redelivery != "fresh_envelope" {
					t.Fatalf("unhandled unacknowledged case: retire %v", c.Retire)
				}
				rig.carriedAgain(rig.interval(), rig.artifact)
				rig.headKept()
				return
			}
			ack := c.Acknowledgement
			err := rig.handle(rigTopic, rig.decision(rigDevice, ack.Outcome, ack.Reason, nil))
			recognised := ack.Outcome == "applied" || slices.Contains(corpus.Vocabulary, ack.Reason)
			switch {
			case recognised && err != nil:
				t.Fatalf("a decision in the vocabulary was refused: %v", err)
			case !recognised && !errors.Is(err, ErrUnrecognisedInboundReason):
				t.Fatalf("a reason outside the vocabulary = %v, want it held as unrecognised", err)
			}
			if c.Retire {
				if c.Redelivery != nil {
					t.Fatalf("a retired head names redelivery %q", *c.Redelivery)
				}
				if rig.sink.Len() != 0 {
					t.Fatal("the head did not retire")
				}
				if carried := rig.drain(); len(carried) != 0 {
					t.Fatal("a retired head was carried again")
				}
				return
			}
			if c.Redelivery == nil || *c.Redelivery != "on_state_change" {
				t.Fatal("unhandled held case")
			}
			rig.held()
			rig.carriedAgain(rig.stateChange(), rig.artifact)
			rig.headKept()
		})
	}
}

// TestUnknownInboundReasonIsHeld sends refusals whose reason is outside the
// vocabulary, including near spellings of retiring reasons: each is held,
// never retired and never carried on the retry interval, and carried again on
// a receiver state change.
func TestUnknownInboundReasonIsHeld(t *testing.T) {
	corpus := loadInboundRetirementCorpus(t)
	reasons := []string{
		"", "Superseded", "SUPERSEDED", " superseded", "superseded ", "superseded\n",
		"superseded\x00", "restatement-conflict", "restatementconflict", "Restatement_Conflict",
		"ѕuperseded", "applied", "queue_full", "retire", "malformed,superseded",
		strings.Repeat("x", 4096), `super"seded`, "superseded\r\nlevel=ERROR", "\x1b[31msuperseded",
		strings.Repeat("\"", 100), "x" + strings.Repeat("\u00e9", 40),
	}
	for _, c := range corpus.Cases {
		if c.Acknowledgement != nil && c.Acknowledgement.Outcome == "refused" && !slices.Contains(corpus.Vocabulary, c.Acknowledgement.Reason) {
			reasons = append(reasons, c.Acknowledgement.Reason)
		}
	}
	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			if slices.Contains(corpus.Vocabulary, reason) {
				t.Fatalf("%q is in the vocabulary", reason)
			}
			rig := newReturnRig(t)
			err := rig.handle(rigTopic, rig.decision(rigDevice, "refused", reason, nil))
			var unrecognised *UnrecognisedInboundReasonError
			if !errors.Is(err, ErrUnrecognisedInboundReason) || !errors.As(err, &unrecognised) {
				t.Fatalf("HandleAck = %v, want the refusal held as unrecognised", err)
			}
			assertQuotedPrefix(t, unrecognised.Reason, reason)
			rig.held()
			rig.carriedAgain(rig.stateChange(), rig.artifact)
			if err := rig.handle(rigTopic, rig.decision(rigDevice, "refused", "superseded", nil)); err != nil || rig.sink.Len() != 0 {
				t.Fatalf("a retiring refusal after the hold = %v with %d queued; want it retired", err, rig.sink.Len())
			}
		})
	}
}

// TestAHoldBindsOnlyTheHeadItWasDecidedOn holds the head, then the head is
// replaced without a wake. The new head is carried on the next interval.
func TestAHoldBindsOnlyTheHeadItWasDecidedOn(t *testing.T) {
	next := validReceiptBytes(rigDevice, 2, 2)
	rig := newReturnRig(t, next)
	if err := rig.handle(rigTopic, rig.decision(rigDevice, "refused", "unknown_key", nil)); err != nil {
		t.Fatal(err)
	}
	rig.held()
	if rig.heldID() != rig.headID {
		t.Fatal("the hold does not name the head it was decided on")
	}
	if err := rig.sink.queue.Remove(rig.headID); err != nil {
		t.Fatal(err)
	}
	rig.carriedAgain(rig.interval(), next)
}

// TestAMisboundRetiringRefusalRetiresNothing sends a retiring reason in a
// decision that is not the runtime's decision on the queued head: it neither
// retires nor holds, and the head is carried again as if unacknowledged.
func TestAMisboundRetiringRefusalRetiresNothing(t *testing.T) {
	const otherDevice = "site-a-edge-02"
	const otherTopic = "ori/site-a-edge-02/evidence/inbound/ack"
	cases := map[string]struct {
		device  string
		topic   string
		outcome string
		mutate  func(*inboundAck)
		payload func([]byte) []byte
	}{
		"applied with a reason":  {outcome: "applied"},
		"capitalised outcome":    {outcome: "Refused"},
		"empty outcome":          {outcome: ""},
		"another artifact":       {outcome: "refused", mutate: func(a *inboundAck) { a.ArtifactDigest = "sha256:" + hex.EncodeToString(make([]byte, 32)) }},
		"another artifact type":  {outcome: "refused", mutate: func(a *inboundAck) { a.ArtifactType = string(AuthorityEpochConfirmation) }},
		"another device in body": {outcome: "refused", mutate: func(a *inboundAck) { a.DeviceID = otherDevice }},
		"another device topic":   {outcome: "refused", topic: otherTopic},
		// Signed correctly by another device on the site, on its own topic,
		// naming this device's head: only the queued-device binding refuses it.
		"another device's signed decision": {outcome: "refused", device: otherDevice, topic: otherTopic},
		"no decision time":                 {outcome: "refused", mutate: func(a *inboundAck) { a.AcknowledgedAtMS = 0 }},
		"tampered after signing":           {outcome: "refused", payload: func(p []byte) []byte { return replaceOnce(p, `"superseded"`, `"malformed"`) }},
		"retiring reason renamed": {outcome: "refused", payload: func(p []byte) []byte {
			return replaceOnce(p, `"reason":"superseded"`, `"reason":"restatement_conflict"`)
		}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rig := newReturnRig(t)
			device := rigDevice
			if c.device != "" {
				device = c.device
			}
			payload := rig.decision(device, c.outcome, "superseded", c.mutate)
			if c.payload != nil {
				payload = c.payload(payload)
			}
			topic := rigTopic
			if c.topic != "" {
				topic = c.topic
			}
			err := rig.handle(topic, payload)
			if err == nil || errors.Is(err, ErrUnrecognisedInboundReason) {
				t.Fatalf("HandleAck = %v, want the decision refused", err)
			}
			rig.headKept()
			if rig.heldID() != "" {
				t.Fatal("a refused decision held the head")
			}
			rig.carriedAgain(rig.interval(), rig.artifact)
			rig.headKept()
		})
	}
}

// documentedReasonBound is the bound docs/OPERATIONS.md gives a logged
// unrecognised reason.
const documentedReasonBound = 64

// assertQuotedPrefix requires reported to be a Go-quoted prefix of at most
// documentedReasonBound bytes of sent, marked as cut exactly when sent is
// longer, with no raw control byte or quote inside the quotes.
func assertQuotedPrefix(t *testing.T, reported, sent string) {
	t.Helper()
	quoted, cut := strings.CutSuffix(reported, "...")
	unquoted, err := strconv.Unquote(quoted)
	if err != nil {
		t.Fatalf("the reported reason %q is not quoted: %v", reported, err)
	}
	if !strings.HasPrefix(sent, unquoted) || len(unquoted) > documentedReasonBound || cut != (len(sent) > documentedReasonBound) ||
		(cut && len(unquoted) != documentedReasonBound) {
		t.Fatalf("the reported reason %q is not the sent reason bounded to %d bytes", reported, documentedReasonBound)
	}
	inner := quoted[1 : len(quoted)-1]
	for i := 0; i < len(inner); i++ {
		if inner[i] < 0x20 || inner[i] == 0x7f {
			t.Fatalf("the reported reason %q carries a raw control byte", reported)
		}
		if inner[i] == '"' && (i == 0 || inner[i-1] != '\\') {
			t.Fatalf("the reported reason %q carries a raw quote", reported)
		}
	}
}

func replaceOnce(payload []byte, from, to string) []byte {
	out := bytes.Replace(payload, []byte(from), []byte(to), 1)
	if bytes.Equal(out, payload) {
		panic("replaceOnce: " + from + " not in payload")
	}
	return out
}

// TestTheDefaultRetryTimerRedeliversAnUnacknowledgedHead runs the publisher as
// the constructor builds it, with its production timer: an unacknowledged head
// is carried again after the retry in a newly signed envelope.
func TestTheDefaultRetryTimerRedeliversAnUnacknowledgedHead(t *testing.T) {
	sink, err := OpenDurableAuthoritySink(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := validReceiptBytes(rigDevice, 1, 1)
	if err := sink.Store(context.Background(), AuthorityArtifact{
		Type: AuthorityDeliveryReceipt, DeviceID: rigDevice, Payload: artifact,
	}); err != nil {
		t.Fatal(err)
	}
	published := make(chan []byte, 128)
	publisher, err := NewReturnPublisher(sink, testAckClock(t, time.Now),
		func(_ context.Context, _ string, _ byte, _ bool, payload []byte) error {
			published <- append([]byte(nil), payload...)
			return nil
		}, rigSecret, "", time.Now, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = publisher.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	var envelopes []inboundEnvelope
	var raw [][]byte
	for len(envelopes) < 2 {
		select {
		case payload := <-published:
			var envelope inboundEnvelope
			if err := json.Unmarshal(payload, &envelope); err != nil {
				t.Fatal(err)
			}
			envelopes = append(envelopes, envelope)
			raw = append(raw, payload)
		case <-time.After(rigWait):
			t.Fatalf("the default retry timer carried the head %d times, want 2", len(envelopes))
		}
	}
	a, b := envelopes[0], envelopes[1]
	if string(a.Artifact) != string(artifact) || string(b.Artifact) != string(artifact) {
		t.Fatal("the redelivery did not carry the same artifact")
	}
	if bytes.Equal(raw[0], raw[1]) || a.Auth == nil || b.Auth == nil || b.Auth.SignedAtMS <= a.Auth.SignedAtMS {
		t.Fatal("the redelivery was not a newly signed envelope")
	}
}
