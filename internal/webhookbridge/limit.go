// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package webhookbridge

import (
	"bytes"
	"container/list"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

// Refusals counts the requests the bridge refused for volume since it
// started. It holds counts only: no sender, number, hash or content.
type Refusals struct {
	sender atomic.Uint64
	global atomic.Uint64
}

// NewRefusals returns zero counts.
func NewRefusals() *Refusals { return &Refusals{} }

// Sender is the requests refused by one sender's rate.
func (r *Refusals) Sender() uint64 { return r.sender.Load() }

// Global is the requests refused by the bridge-wide ceiling.
func (r *Refusals) Global() uint64 { return r.global.Load() }

// unreadSender is the key of every sender the bridge cannot read the way the
// runtime does: a `from` that is not a JSON string, or a body or form pair the
// bridge cannot decode while it names a sender field. These share one
// per-sender bucket instead of drawing on the ceiling alone, so no spelling
// the bridge cannot follow escapes a per-sender limit. It cannot collide with
// a read sender, which holds only digits and `+`.
const unreadSender = "unread"

// senderOf reads the sender from a provider body only to key the limiter; the
// body is forwarded unchanged. It reads the runtime's way only where the two
// readings provably agree: a JSON content type is read as a JSON object and
// anything else as a form, `from` is taken when it is a non-empty string and
// `from_number` otherwise, and the result keeps only digits and `+`. Any other
// shape that names a sender field fails closed to unreadSender. A body naming
// no sender field, such as a delivery report, has the empty sender.
func senderOf(contentType string, body []byte) string {
	if strings.Contains(strings.ToLower(contentType), "application/json") {
		return jsonSender(body)
	}
	return formSender(body)
}

func jsonSender(body []byte) string {
	var payload map[string]json.RawMessage
	if json.Unmarshal(body, &payload) != nil {
		// The runtime may still read a sender here (it accepts NaN, for one).
		if bytes.Contains(body, []byte("from")) || bytes.Contains(body, []byte(`\u`)) {
			return unreadSender
		}
		return ""
	}
	from, fromSet := payload["from"]
	fromNumber, numberSet := payload["from_number"]
	if !fromSet && !numberSet {
		for key := range payload {
			// A key the decoder matched only by unescaping, or a near miss,
			// is not followed: the runtime decodes keys its own way.
			if strings.HasPrefix(key, "from") {
				return unreadSender
			}
		}
		return ""
	}
	// Only a string, or an absent or null `from`, has the one reading both
	// sides agree on; anything else, a number among them, fails closed.
	var sender string
	if fromSet && !isJSONNull(from) {
		if json.Unmarshal(from, &sender) != nil {
			return unreadSender
		}
	}
	if sender == "" && numberSet && !isJSONNull(fromNumber) {
		if json.Unmarshal(fromNumber, &sender) != nil {
			return unreadSender
		}
	}
	return readSender(sender, fromSet || numberSet)
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func formSender(body []byte) string {
	values, err := url.ParseQuery(string(body))
	if err != nil && namesSenderField(body) {
		// A pair the bridge cannot decode may be the sender the runtime reads.
		return unreadSender
	}
	from := values.Get("from")
	if from == "" {
		from = values.Get("from_number")
	}
	return readSender(from, values.Has("from") || values.Has("from_number"))
}

// namesSenderField reports whether a form body has a pair that Go's form
// reader drops and whose key could read as a sender field, or whose key it
// cannot decode at all: the runtime's reader may keep that pair.
func namesSenderField(body []byte) bool {
	for pair := range strings.SplitSeq(string(body), "&") {
		key, value, _ := strings.Cut(pair, "=")
		unescapedKey, keyErr := url.QueryUnescape(key)
		if keyErr != nil || strings.Contains(key, ";") {
			return true
		}
		if !strings.HasPrefix(unescapedKey, "from") {
			continue
		}
		if _, valueErr := url.QueryUnescape(value); valueErr != nil || strings.Contains(value, ";") {
			return true
		}
	}
	return false
}

// readSender normalises a sender the bridge read. A sender field that was
// present but reads as nothing fails closed; a body without one has the
// empty sender.
func readSender(value string, present bool) string {
	if sender := normalizeSender(value); sender != "" {
		return sender
	}
	if present {
		return unreadSender
	}
	return ""
}

// normalizeSender keeps digits and `+`, as the runtime normalises a phone
// number. Go's decimal digits are a subset of what the runtime keeps, so two
// spellings are one sender here whenever they are one sender there.
func normalizeSender(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsDigit(r) || r == '+' {
			return r
		}
		return -1
	}, value)
}

type bucket struct {
	tokens float64
	last   time.Time
}

// refund returns a token taken for a request that was then refused elsewhere.
func (b *bucket) refund(burst float64) {
	b.tokens = min(burst, b.tokens+1)
}

func (b *bucket) take(now time.Time, perSecond, burst float64) bool {
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(burst, b.tokens+elapsed*perSecond)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type senderKey [sha256.Size]byte

type senderEntry struct {
	key    senderKey
	bucket bucket
}

// limiter is the bridge's volume bound: a token bucket per sender, keyed by
// an HMAC of the normalised sender under a key that never leaves the process,
// and a bridge-wide ceiling. Sender state is bounded: above maxSenders the
// least recently seen sender is evicted, and a returning sender starts with a
// full bucket, which the bridge-wide ceiling still bounds.
type limiter struct {
	hashKey []byte

	senderPerSecond float64
	senderBurst     float64
	globalPerSecond float64
	globalBurst     float64
	maxSenders      int

	mu      sync.Mutex
	order   *list.List
	senders map[senderKey]*list.Element
	global  bucket
}

func newLimiter(senderPerMinute, senderBurst, globalPerMinute, globalBurst, maxSenders int, now time.Time) (*limiter, error) {
	hashKey := make([]byte, 32)
	if _, err := rand.Read(hashKey); err != nil {
		return nil, err
	}
	return &limiter{
		hashKey:         hashKey,
		senderPerSecond: float64(senderPerMinute) / 60,
		senderBurst:     float64(senderBurst),
		globalPerSecond: float64(globalPerMinute) / 60,
		globalBurst:     float64(globalBurst),
		maxSenders:      maxSenders,
		order:           list.New(),
		senders:         make(map[senderKey]*list.Element),
		global:          bucket{tokens: float64(globalBurst), last: now},
	}, nil
}

func (l *limiter) keyOf(sender string) senderKey {
	mac := hmac.New(sha256.New, l.hashKey)
	_, _ = mac.Write([]byte(sender))
	var key senderKey
	copy(key[:], mac.Sum(nil))
	return key
}

// verdict is the outcome of admitting one request.
type verdict int

const (
	admitted verdict = iota
	refusedBySender
	refusedByGlobal
)

// admit charges the sender's bucket, then the bridge-wide one. A request
// without a sender, such as a delivery report, draws on the ceiling alone. A
// request the sender's bucket refuses does not draw on the ceiling, and one
// the ceiling refuses has its sender's token returned, so a flood of others
// never spends an operator's own allowance.
func (l *limiter) admit(sender string, now time.Time) verdict {
	l.mu.Lock()
	defer l.mu.Unlock()
	if sender == "" {
		if !l.global.take(now, l.globalPerSecond, l.globalBurst) {
			return refusedByGlobal
		}
		return admitted
	}
	key := l.keyOf(sender)
	element, ok := l.senders[key]
	if ok {
		l.order.MoveToFront(element)
	} else {
		if l.order.Len() >= l.maxSenders {
			oldest := l.order.Back()
			l.order.Remove(oldest)
			delete(l.senders, oldest.Value.(*senderEntry).key)
		}
		element = l.order.PushFront(&senderEntry{key: key, bucket: bucket{tokens: l.senderBurst, last: now}})
		l.senders[key] = element
	}
	own := &element.Value.(*senderEntry).bucket
	if !own.take(now, l.senderPerSecond, l.senderBurst) {
		return refusedBySender
	}
	if !l.global.take(now, l.globalPerSecond, l.globalBurst) {
		own.refund(l.senderBurst)
		return refusedByGlobal
	}
	return admitted
}

// tracked is the number of senders holding state.
func (l *limiter) tracked() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.order.Len()
}
