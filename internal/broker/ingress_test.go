// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

// ingressTopics are a concrete topic for each family the gateway receives, on
// a device whose name must never reach a refusal record.
var ingressTopics = map[string]string{
	contracts.FamilyRuntimeHeartbeat:   "ori/dev-secret-7/runtime/heartbeat",
	contracts.FamilyReasoningRequest:   "ori/dev-secret-7/reasoning/request",
	contracts.FamilyEnrichmentRequest:  "ori/dev-secret-7/tier_c/enrichment/request",
	contracts.FamilyExportResponse:     "ori/dev-secret-7/export/response/req-1",
	contracts.FamilyEvidenceOutbound:   "ori/dev-secret-7/evidence/outbound",
	contracts.FamilyEvidenceInboundAck: "ori/dev-secret-7/evidence/inbound/ack",
}

type ingressMessage struct {
	topic   string
	payload []byte
}

func (m ingressMessage) Duplicate() bool   { return false }
func (m ingressMessage) Qos() byte         { return QoSReasoning }
func (m ingressMessage) Retained() bool    { return false }
func (m ingressMessage) Topic() string     { return m.topic }
func (m ingressMessage) MessageID() uint16 { return 1 }
func (m ingressMessage) Payload() []byte   { return m.payload }
func (m ingressMessage) Ack()              {}

type ingressRig struct {
	client  *Client
	logs    *lockedBuffer
	clockMu sync.Mutex
	clock   time.Time
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newIngressRig(t *testing.T) *ingressRig {
	t.Helper()
	logs := &lockedBuffer{}
	c, err := New(Options{
		BrokerURL: "tcp://127.0.0.1:1", ClientID: "ingress",
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &ingressRig{client: c, logs: logs, clock: time.Unix(1_787_000_000, 0)}
}

func (r *ingressRig) now() time.Time {
	r.clockMu.Lock()
	defer r.clockMu.Unlock()
	return r.clock
}

func (r *ingressRig) advance(d time.Duration) {
	r.clockMu.Lock()
	r.clock = r.clock.Add(d)
	r.clockMu.Unlock()
}

// deliver hands payload to a subscription on topic through the client's real
// message wrapper and reports whether the handler ran.
func (r *ingressRig) subscription(t *testing.T, topic string) (deliver func([]byte) bool) {
	t.Helper()
	gate, err := newIngressGate(topic, nil, r.client.ingress, r.now)
	if err != nil {
		t.Fatal(err)
	}
	var ran bool
	wrapped := r.client.wrapHandler(subscription{qos: QoSReasoning, gate: gate, handler: func(string, []byte) { ran = true }})
	return func(payload []byte) bool {
		ran = false
		wrapped(nil, ingressMessage{topic: topic, payload: payload})
		return ran
	}
}

func jsonObjectPayload(length int) []byte {
	payload := make([]byte, 0, length)
	payload = append(payload, `{"pad":"`...)
	payload = append(payload, bytes.Repeat([]byte("a"), length-len(`{"pad":""}`))...)
	return append(payload, `"}`...)
}

func TestEveryReceivedFamilyHasAMaximumAndARate(t *testing.T) {
	for _, family := range contracts.ReceivedFamilies {
		if err := CheckSubscription(ingressTopics[family]); err != nil {
			t.Errorf("%s: %v", family, err)
		}
	}
	if len(IngressRates) != len(contracts.ReceivedFamilies) {
		t.Fatalf("%d families have a rate, want the %d the gateway receives", len(IngressRates), len(contracts.ReceivedFamilies))
	}
	// A family the gateway only produces, and a topic outside the table, are
	// never subscribed.
	for _, topic := range []string{"ori/gateway/health", "ori/dev-01/reasoning/response", "ori/dev-01/evidence/inbound", "ori/dev-01/unknown", "ori/#", "ori/dev-01/export/response/#"} {
		if err := CheckSubscription(topic); !errors.Is(err, ErrNoPayloadMaximum) {
			t.Errorf("CheckSubscription(%q) = %v, want ErrNoPayloadMaximum", topic, err)
		}
	}
}

func TestSubscribeRefusesATopicWithoutAMaximum(t *testing.T) {
	r := newIngressRig(t)
	err := r.client.Subscribe(t.Context(), "ori/dev-01/unbounded", QoSReasoning, func(string, []byte) {})
	if !errors.Is(err, ErrNoPayloadMaximum) {
		t.Fatalf("Subscribe = %v, want ErrNoPayloadMaximum before any connection check", err)
	}
}

// TestAPayloadOverItsTopicMaximumIsRefusedBeforeParsing drives every received
// family through the real wrapper: at the maximum the handler runs, one byte
// over it never does, whatever the bytes are.
func TestAPayloadOverItsTopicMaximumIsRefusedBeforeParsing(t *testing.T) {
	for _, family := range contracts.ReceivedFamilies {
		t.Run(family, func(t *testing.T) {
			r := newIngressRig(t)
			deliver := r.subscription(t, ingressTopics[family])
			maximum := contracts.PayloadMaxima[family]
			if !deliver(jsonObjectPayload(maximum)) {
				t.Fatal("a payload at the maximum was not handled")
			}
			if deliver(jsonObjectPayload(maximum + 1)) {
				t.Fatal("a payload one byte over the maximum reached its handler")
			}
			if deliver(bytes.Repeat([]byte{0xFF}, maximum+1)) {
				t.Fatal("invalid UTF-8 one byte over the maximum reached its handler")
			}
			if got := r.client.ingress.Snapshot()[family]; got.PayloadTooLarge != 2 || got.VolumeDropped != 0 {
				t.Fatalf("counts = %+v, want two size refusals", got)
			}
		})
	}
}

type sizeCorpus struct {
	Cases []struct {
		Name              string `json:"name"`
		Topic             string `json:"topic"`
		Direction         string `json:"direction"`
		ConfiguredMaximum *int   `json:"configured_maximum"`
		Construction      string `json:"construction"`
		Length            int    `json:"length"`
		SHA256            string `json:"sha256"`
		Verdict           string `json:"verdict"`
	} `json:"cases"`
}

// TestPayloadSizeVectorCases drives every vendored case for a family the
// gateway receives, at the table's maximum, through the real wrapper. The
// gateway offers no setting for a maximum, so a case with a configured
// maximum has nothing here to configure.
func TestPayloadSizeVectorCases(t *testing.T) {
	raw, err := specvectors.Read("gateway-api/vectors/payload-size-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus sizeCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	received := map[string]bool{}
	for _, family := range contracts.ReceivedFamilies {
		received[family] = true
	}
	names := map[string]string{}
	for _, c := range corpus.Cases {
		names[c.Name] = c.Topic
	}
	checkLANPinsExist(t, lanSizeCasePins, names)
	seen := map[string]int{}
	for _, c := range corpus.Cases {
		observed := ""
		if c.Direction == "runtime_to_gateway" != received[c.Topic] {
			observed = "direction " + c.Direction + " disagrees with the families the gateway receives"
		} else if want, ok := contracts.PayloadMaxima[c.Topic]; !ok {
			observed = "topic is not in the gateway's table"
		} else if c.ConfiguredMaximum == nil && c.Verdict == "admitted" && c.Length > want {
			observed = fmt.Sprintf("admitted at %d over the gateway's maximum %d", c.Length, want)
		}
		if checkLANPin(t, lanSizeCasePins, c.Name, observed) {
			continue
		}
		if !received[c.Topic] || c.ConfiguredMaximum != nil {
			continue
		}
		var payload []byte
		switch c.Construction {
		case "json_object":
			payload = jsonObjectPayload(c.Length)
		case "invalid_utf8":
			payload = bytes.Repeat([]byte{0xFF}, c.Length)
		default:
			t.Fatalf("%s: unknown construction %q", c.Name, c.Construction)
		}
		if sum := sha256.Sum256(payload); hex.EncodeToString(sum[:]) != c.SHA256 {
			t.Fatalf("%s: built payload digest differs from the corpus", c.Name)
		}
		r := newIngressRig(t)
		handled := r.subscription(t, ingressTopics[c.Topic])(payload)
		if handled != (c.Verdict == "admitted") {
			t.Fatalf("%s: handled=%v, want verdict %s", c.Name, handled, c.Verdict)
		}
		seen[c.Topic]++
	}
	for family := range received {
		if seen[family] < 4 {
			t.Errorf("%s: %d table cases driven, want the at, over, four-times and invalid_utf8 cases", family, seen[family])
		}
	}
	for family, maximum := range contracts.PayloadMaxima {
		if maximum < contracts.PayloadFloor {
			t.Errorf("%s: maximum %d is below the floor", family, maximum)
		}
	}
}

// TestAnOversizePayloadCostsNoParse refuses payloads far over the maximum
// with a fixed allocation count, the same for every size: nothing reads past
// the length.
func TestAnOversizePayloadCostsNoParse(t *testing.T) {
	r := newIngressRig(t)
	topic := ingressTopics[contracts.FamilyReasoningRequest]
	deliver := r.subscription(t, topic)
	maximum := contracts.PayloadMaxima[contracts.FamilyReasoningRequest]
	small := bytes.Repeat([]byte(`\`), maximum+1)
	huge := bytes.Repeat([]byte(`\`), 64*maximum)
	deliver(small) // the family's first refusal creates its counters
	allocs := func(payload []byte) float64 {
		return testing.AllocsPerRun(50, func() {
			if deliver(payload) {
				t.Fatal("an oversize payload reached its handler")
			}
		})
	}
	smallAllocs, hugeAllocs := allocs(small), allocs(huge)
	if hugeAllocs != smallAllocs || hugeAllocs > 2 {
		t.Fatalf("refusal allocates %v for %d bytes and %v for %d bytes, want a small constant", smallAllocs, len(small), hugeAllocs, len(huge))
	}
}

func TestAVolumeOverTheRateIsDroppedAndCounted(t *testing.T) {
	for _, family := range contracts.ReceivedFamilies {
		t.Run(family, func(t *testing.T) {
			r := newIngressRig(t)
			deliver := r.subscription(t, ingressTopics[family])
			rate := IngressRates[family]
			for i := range rate.Burst {
				if !deliver([]byte("{}")) {
					t.Fatalf("message %d within the burst was dropped", i)
				}
			}
			if deliver([]byte("{}")) {
				t.Fatal("a message past the burst reached its handler")
			}
			r.advance(time.Duration(float64(time.Second) / rate.PerSecond))
			if !deliver([]byte("{}")) {
				t.Fatal("a message after the rate refilled was dropped")
			}
			if got := r.client.ingress.Snapshot()[family]; got.VolumeDropped != 1 || got.PayloadTooLarge != 0 {
				t.Fatalf("counts = %+v, want one volume drop", got)
			}
		})
	}
}

// TestASubscriptionsRateIsItsOwn keeps one device's flood from dropping
// another device's messages on a per-device subscription.
func TestASubscriptionsRateIsItsOwn(t *testing.T) {
	r := newIngressRig(t)
	flooding := r.subscription(t, "ori/dev-01/runtime/heartbeat")
	quiet := r.subscription(t, "ori/dev-02/runtime/heartbeat")
	for range 100 {
		flooding([]byte("{}"))
	}
	if !quiet([]byte("{}")) {
		t.Fatal("another device's flood dropped this device's heartbeat")
	}
}

// TestAWildcardSubscriptionRatesEachDevice floods a wildcard subscription
// from an unconfigured device and from many forged device names, and from one
// configured device: another configured device, even one that has sent
// nothing yet, keeps its whole burst, and the flood adds no bucket.
func TestAWildcardSubscriptionRatesEachDevice(t *testing.T) {
	for _, filter := range []string{contracts.EvidenceOutboundTopicFilter, contracts.GatewayReasoningRequestTopicFilter} {
		t.Run(filter, func(t *testing.T) {
			r := newIngressRig(t)
			gate, err := newIngressGate(filter, []string{"dev-01", "dev-02"}, r.client.ingress, r.now)
			if err != nil {
				t.Fatal(err)
			}
			var ran bool
			wrapped := r.client.wrapHandler(subscription{qos: QoSReasoning, gate: gate, handler: func(string, []byte) { ran = true }})
			deliver := func(device string) bool {
				ran = false
				topic := strings.Replace(filter, "+", device, 1)
				wrapped(nil, ingressMessage{topic: topic, payload: []byte("{}")})
				return ran
			}
			burst := IngressRates[gate.family].Burst
			for range 10 * burst {
				deliver("not-configured")
				deliver("dev-01")
			}
			for i := range 1000 {
				deliver(fmt.Sprintf("forged-%d-%s", i, strings.Repeat("x", 1000)))
			}
			for i := range burst {
				if !deliver("dev-02") {
					t.Fatalf("dev-02's message %d was dropped by other senders' floods", i)
				}
			}
			if deliver("dev-02") {
				t.Fatal("dev-02 exceeded its own burst")
			}
			if len(gate.buckets) != 2 {
				t.Fatalf("%d buckets kept, want one per configured device", len(gate.buckets))
			}
		})
	}
}

// TestIngressRefusalsAreRecordedWithoutContent floods every family and
// requires the log to name only the family, the count and the size classes,
// paced to a line per family and reason.
func TestIngressRefusalsAreRecordedWithoutContent(t *testing.T) {
	r := newIngressRig(t)
	const marker = "PAYLOAD-MARKER"
	started := time.Now()
	for _, family := range contracts.ReceivedFamilies {
		deliver := r.subscription(t, ingressTopics[family])
		maximum := contracts.PayloadMaxima[family]
		oversize := append([]byte(marker), bytes.Repeat([]byte("a"), maximum)...)
		for range 200 {
			deliver(oversize)
		}
		for range IngressRates[family].Burst + 200 {
			deliver([]byte(`{"m":"` + marker + `"}`))
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		logs := r.logs.String()
		complete := true
		for _, family := range contracts.ReceivedFamilies {
			complete = complete && strings.Count(logs, "family="+family+" refused=") >= 1 &&
				strings.Contains(logs, "family="+family+" dropped=")
		}
		if complete && sumLogged(logs, "refused=") == 200*uint64(len(contracts.ReceivedFamilies)) &&
			sumLogged(logs, "dropped=") == 200*uint64(len(contracts.ReceivedFamilies)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refusals were not all logged:\n%s", logs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	logs := r.logs.String()
	if strings.Contains(logs, marker) || strings.Contains(logs, "dev-secret-7") {
		t.Fatalf("a refusal record carries payload content or a device identifier:\n%s", logs)
	}
	// Each family and reason writes its first refusal at once and the rest in
	// at most one line per pacing interval the flood lasted.
	intervals := int(time.Since(started)/ingressLogInterval) + 1
	if lines := strings.Count(logs, "\n"); lines > 2*len(contracts.ReceivedFamilies)*(1+intervals) {
		t.Fatalf("%d log lines for a flood, want the log paced per family and reason", lines)
	}
}

func sumLogged(logs, key string) uint64 {
	var total uint64
	for line := range strings.SplitSeq(logs, "\n") {
		_, rest, found := strings.Cut(line, key)
		if !found {
			continue
		}
		var n uint64
		for _, ch := range rest {
			if ch < '0' || ch > '9' {
				break
			}
			n = n*10 + uint64(ch-'0')
		}
		total += n
	}
	return total
}

func TestSizeClassIsTheLeastPowerOfTwoNotBelowTheLength(t *testing.T) {
	for length, want := range map[int]int{0: 1, 1: 1, 2: 2, 3: 4, 4096: 4096, 4097: 8192, 262145: 524288} {
		if got := contracts.SizeClass(length); got != want {
			t.Errorf("SizeClass(%d) = %d, want %d", length, got, want)
		}
	}
}

// TestAByteBudgetBoundsTheParseWorkEachDeviceCanCause sends payloads at the
// family maximum until the device's byte budget is spent: the next is dropped
// as volume, while a payload at the maximum always fits a full budget.
func TestAByteBudgetBoundsTheParseWorkEachDeviceCanCause(t *testing.T) {
	for _, family := range contracts.ReceivedFamilies {
		t.Run(family, func(t *testing.T) {
			rate := IngressRates[family]
			maximum := contracts.PayloadMaxima[family]
			if rate.BurstBytes < maximum {
				t.Fatalf("burst %d bytes is below the family maximum %d", rate.BurstBytes, maximum)
			}
			r := newIngressRig(t)
			deliver := r.subscription(t, ingressTopics[family])
			payload := jsonObjectPayload(maximum)
			fits := min(rate.BurstBytes/maximum, rate.Burst)
			for i := range fits {
				if !deliver(payload) {
					t.Fatalf("payload %d within the budget was dropped", i)
				}
			}
			if fits < rate.Burst && deliver(payload) {
				t.Fatal("a payload past the byte budget was handled")
			}
			r.advance(time.Duration(float64(maximum) / rate.BytesPerSecond * float64(time.Second)))
			if !deliver(payload) {
				t.Fatal("a payload after the budget refilled was dropped")
			}
		})
	}
}
