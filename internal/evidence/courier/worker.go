// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"context"
	"errors"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/faults"
	"github.com/ori-platform/ori-gateway/internal/evidence/refusal"
	"log/slog"
	"sort"
	"sync"
	"time"
)

const (
	defaultMaxBackoff = 5 * time.Minute
	// maxBackoffAttempts bounds the persisted attempt count; the delay stops
	// growing long before it, at the maximum back-off.
	maxBackoffAttempts = 1 << 16
)

// DeliveryWorker drains the durable queue through the independent evidence
// channel. Each device has two delivery lanes, its anchor registrations and
// every other artifact it delivers, and order, retry state, back-off and holds
// are kept per device and lane (evidence-transport/v2, refusal policy): a hold
// or back-off in one lane never delays the device's other lane or another
// device. Within a lane, failure leaves the head intact and refusal.Policy
// decides whether it is held, retried on the device's next handoff, or backed
// off. A held head is never discarded and never re-sent by this process; a
// later process re-sends it only while no durable hold exists.
type DeliveryWorker struct {
	queue       *DurableQueue
	channel     EvidenceChannel
	sink        AuthoritySink
	retry       time.Duration
	backoffBase time.Duration
	maxBackoff  time.Duration
	reminder    time.Duration
	log         *slog.Logger
	faults      *faults.Recorder

	mu      sync.Mutex
	lanes   map[LaneKey]*deliveryLane
	runCtx  context.Context
	running sync.WaitGroup
}

type DeliveryWorkerOptions struct {
	// RetryInterval is the delivery interval: the idle poll, and the least
	// any back-off waits.
	RetryInterval time.Duration
	// BackoffBase is the first back-off delay; it doubles per consecutive
	// back-off, up to MaxBackoff. evidence-transport/v2 requires
	// MaxBackoff >= BackoffBase >= RetryInterval; configuration refuses
	// anything else, and the worker raises a smaller value to meet it.
	BackoffBase time.Duration
	MaxBackoff  time.Duration
	// Faults is the active courier-fault state reported with Status. It is
	// shared with the queues and the admission path.
	Faults *faults.Recorder
	// BlockedReminderInterval is how often a held or stalled lane is logged
	// again while it stays so, making the state visible in the log without
	// site health.
	BlockedReminderInterval time.Duration
	Logger                  *slog.Logger
}

type channelRefusalError struct {
	retryAfter time.Duration
}

func (e *channelRefusalError) Error() string { return errChannelRefused.Error() }
func (e *channelRefusalError) Unwrap() error { return errChannelRefused }

func NewDeliveryWorker(queue *DurableQueue, channel EvidenceChannel, sink AuthoritySink, opts DeliveryWorkerOptions) (*DeliveryWorker, error) {
	if queue == nil {
		return nil, fmt.Errorf("evidence: delivery worker requires a durable queue")
	}
	if channel == nil {
		return nil, fmt.Errorf("evidence: delivery worker requires an independent evidence channel")
	}
	retry := opts.RetryInterval
	if retry <= 0 {
		retry = defaultDeliveryRetry
	}
	base := max(opts.BackoffBase, retry)
	maxBackoff := opts.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = max(defaultMaxBackoff, base)
	}
	maxBackoff = max(maxBackoff, base)
	reminder := opts.BlockedReminderInterval
	if reminder <= 0 {
		reminder = defaultBlockedReminder
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	w := &DeliveryWorker{
		queue: queue, channel: channel, sink: sink, retry: retry, backoffBase: base, maxBackoff: maxBackoff,
		reminder: reminder, log: logger, lanes: make(map[LaneKey]*deliveryLane), faults: opts.Faults,
	}
	// Every lane with queued artifacts exists from the start, so a restored
	// hold or back-off is reported before the first attempt.
	for _, key := range queue.Lanes() {
		w.lane(key).restore()
	}
	return w, nil
}

// lane returns a device's lane, creating it, and starting it when the worker
// is already running.
func (w *DeliveryWorker) lane(key LaneKey) *deliveryLane {
	w.mu.Lock()
	defer w.mu.Unlock()
	if l, ok := w.lanes[key]; ok {
		return l
	}
	l := &deliveryLane{w: w, device: key.Device, lane: key.Lane, wake: make(chan struct{}, 1)}
	w.lanes[key] = l
	if w.runCtx != nil {
		w.startLocked(l)
	}
	return l
}

func (w *DeliveryWorker) startLocked(l *deliveryLane) {
	ctx := w.runCtx
	w.running.Go(func() {
		l.run(ctx)
	})
}

// NotifyDevice is a handoff for one device: a newly admitted artifact for it.
// It is a handoff for both of that device's lanes and for no other device's:
// it retries a lane waiting for a handoff, and never re-sends a held head or
// brings a back-off forward. It is non-blocking.
func (w *DeliveryWorker) NotifyDevice(device string) {
	if w == nil {
		return
	}
	for _, lane := range []Lane{LaneRegistration, LaneEvidence} {
		w.lane(LaneKey{Device: device, Lane: lane}).notify()
	}
}

func (l *deliveryLane) notify() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// Run drives every lane until ctx ends. A process start is a handoff
// for every lane: each attempts its head unless that head is held or its
// persisted back-off has not elapsed.
func (w *DeliveryWorker) Run(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("evidence: nil delivery worker")
	}
	w.mu.Lock()
	if w.runCtx != nil {
		w.mu.Unlock()
		return fmt.Errorf("evidence: delivery worker is already running")
	}
	w.runCtx = ctx
	for _, l := range w.lanes {
		w.startLocked(l)
	}
	w.mu.Unlock()
	<-ctx.Done()
	w.running.Wait()
	return nil
}

// backoffDelay is the wait before the next attempt after the given number of
// consecutive back-offs: the base doubling to the bound, and never shorter
// than the delivery interval or a Retry-After the response carried.
func (w *DeliveryWorker) backoffDelay(attempts int, floor time.Duration) time.Duration {
	delay := w.backoffBase
	for i := 1; i < attempts && delay < w.maxBackoff; i++ {
		delay *= 2
	}
	if delay > w.maxBackoff {
		delay = w.maxBackoff
	}
	return max(delay, w.retry, floor)
}

// deliveryLane is one device's lane: one delivery order, with its own retry
// state and hold.
type deliveryLane struct {
	w      *DeliveryWorker
	device string
	lane   Lane
	wake   chan struct{}

	deliveryMu    sync.Mutex
	mu            sync.Mutex
	lastFailureAt time.Time
	lastError     string
	// notBefore is when a backed-off head may next be sent; attempts is the
	// consecutive back-off count that sizes the next delay.
	notBefore time.Time
	attempts  int
	held      *heldHead
	stall     *stallState
	// attempted and attemptReason describe the head the last delivery attempt
	// named, so a failure can be logged against it.
	attempted     QueuedArtifact
	attemptReason string
}

// heldHead is a lane head the evidence authority refused terminally, and its
// durable diagnosis.
type heldHead struct {
	record    HoldRecord
	deviceID  string
	persisted bool
}

// stallState is a lane head the worker could not deliver and does not hold.
type stallState struct {
	id              string
	digest          string
	device          string
	lane            Lane
	failure         string
	reason          string
	waitsForHandoff bool
	since           time.Time
	lastLogged      time.Time
}

func (l *deliveryLane) key() LaneKey { return LaneKey{Device: l.device, Lane: l.lane} }

// queued is the number of artifacts in the lane.
func (l *deliveryLane) queued() int { return l.w.queue.LenLane(l.key()) }

// head is the only place the lane chooses what it offers next: the oldest
// active artifact in the lane. A hold at the head of either lane holds
// everything behind it: a terminal refusal leaves the lane only by an archival
// on a verified retention (evidence-transport/v2, refusal policy), which this
// courier does not yet verify, so without one it is held.
func (l *deliveryLane) head() (QueuedArtifact, bool) { return l.w.queue.PeekLane(l.key()) }

// retired is the only place a retirement becomes a handoff: a 200 retiring an
// anchor registration is a handoff for the same device's evidence lane, whether
// it carried an epoch confirmation or was accepted pending, since the evidence
// lane may be waiting on the epoch it confirmed. The registration lane itself
// is already draining.
func (l *deliveryLane) retired(queued QueuedArtifact) {
	if queued.Type == ArtifactAnchorRegistration {
		l.w.lane(LaneKey{Device: l.device, Lane: LaneEvidence}).notify()
	}
}

// restore adopts a persisted hold or back-off for the lane's head, without
// sending it.
func (l *deliveryLane) restore() {
	head, ok := l.adoptedHead()
	if !ok {
		return
	}
	record, ok := l.w.queue.BackoffFor(head.ID)
	if !ok {
		return
	}
	notBefore := time.UnixMilli(record.NotBeforeMS)
	// A clock that moved backwards must not stretch a back-off past its bound.
	if latest := time.Now().Add(l.w.maxBackoff); notBefore.After(latest) {
		notBefore = latest
	}
	l.mu.Lock()
	l.notBefore, l.attempts = notBefore, record.Attempts
	l.lastError = safeFailureReason(errChannelRefused)
	l.lastFailureAt = time.Now()
	l.mu.Unlock()
}

func (l *deliveryLane) run(ctx context.Context) {
	first := time.Duration(0)
	l.mu.Lock()
	if wait := time.Until(l.notBefore); wait > 0 {
		first = wait
	}
	l.mu.Unlock()
	timer := time.NewTimer(first)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.wake:
			if l.backingOff() {
				// A handoff never brings a back-off forward; the timer is
				// already set for the earliest permitted retry.
				continue
			}
		case <-timer.C:
			// A held head, or one waiting for a handoff, is only ever reminded
			// about when the timer fires; it is never retried on a timer.
			if l.remind() {
				resetTimer(timer, l.w.reminder)
				continue
			}
		}

		delivered, err := l.deliverHead(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return
			}
			if errors.Is(err, errHeadHeld) {
				// No attempt was made. The reminder timer keeps running; a stream
				// of handoffs must not postpone it.
				continue
			}
			l.recordFailure(err)
			if errors.Is(err, errChannelPermanentRefusal) {
				l.clearBackoff()
				resetTimer(timer, l.w.reminder)
				continue
			}
			l.noteStall(err)
			if errors.Is(err, errChannelReceiverState) {
				// Retried on the next handoff, never on a timer. The timer
				// now only reminds.
				l.clearBackoff()
				resetTimer(timer, l.w.reminder)
				continue
			}
			resetTimer(timer, l.scheduleBackoff(err))
			continue
		}
		l.clearFailure()
		if delivered {
			// Drain immediately while work exists. No timer is needed to make
			// progress, and each successful removal is durably committed first.
			resetTimer(timer, 0)
			continue
		}
		resetTimer(timer, l.w.retry)
	}
}

// scheduleBackoff sets and persists the lane's next permitted attempt: bounded
// exponential back-off, never shorter than a Retry-After the response carried.
func (l *deliveryLane) scheduleBackoff(err error) time.Duration {
	floor := time.Duration(0)
	if refusal, ok := errors.AsType[*channelRefusalError](err); ok {
		floor = refusal.retryAfter
	}
	l.mu.Lock()
	if l.attempts < maxBackoffAttempts {
		l.attempts++
	}
	attempts := l.attempts
	head := l.attempted
	l.mu.Unlock()
	delay := l.w.backoffDelay(attempts, floor)
	notBefore := time.Now().Add(delay)
	l.mu.Lock()
	l.notBefore = notBefore
	l.mu.Unlock()
	if head.ID != "" {
		if err := l.w.queue.SetBackoff(BackoffRecord{
			QueueRecord: head.ID, NotBeforeMS: notBefore.UnixMilli(), Attempts: attempts,
		}); err != nil && !errors.Is(err, ErrArtifactNotFound) {
			// A back-off that is not persisted is still kept by this process;
			// a restart would retry the head once, sooner than it should.
			l.w.log.Warn("evidence delivery back-off was not persisted",
				"artifact_digest", payloadDigest(head.Payload), "queue_record", head.ID, "device_id", l.device,
				"lane", string(l.lane))
		}
	}
	return delay
}

func (l *deliveryLane) clearBackoff() {
	l.mu.Lock()
	l.notBefore, l.attempts = time.Time{}, 0
	head := l.attempted
	l.mu.Unlock()
	if head.ID != "" {
		_ = l.w.queue.ClearBackoff(head.ID)
	}
}

// backingOff reports whether a back-off still forbids an attempt.
func (l *deliveryLane) backingOff() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return time.Now().Before(l.notBefore)
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	stopTimer(timer)
	timer.Reset(delay)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (l *deliveryLane) deliverHead(ctx context.Context) (bool, error) {
	l.deliveryMu.Lock()
	defer l.deliveryMu.Unlock()

	queued, ok := l.adoptedHead()
	if l.releaseHeldUnless(queued, ok) {
		return false, errHeadHeld
	}
	if !ok {
		return false, nil
	}
	l.mu.Lock()
	if l.attempted.ID != queued.ID {
		// A new head starts its own back-off from the beginning.
		l.attempts = 0
	}
	l.attempted, l.attemptReason, l.notBefore = queued, "", time.Time{}
	l.mu.Unlock()
	result, err := l.w.channel.Deliver(ctx, queued)
	if err != nil {
		if errors.Is(err, ErrUnrecognisedOutcome) {
			l.setAttemptReason(refusal.Unrecognised)
			return false, errMalformedResponse
		}
		return false, errChannelUnavailable
	}
	if !result.Accepted {
		l.setAttemptReason(refusal.RecordedReason(result.RefusalStatus, result.RefusalReason))
		if !result.Retriable {
			if result.ReceiverState {
				return false, errChannelReceiverState
			}
			l.hold(queued, result.RefusalStatus, result.RefusalReason)
			return false, errChannelPermanentRefusal
		}
		return false, &channelRefusalError{retryAfter: result.RetryAfter}
	}
	sink := l.w.sink
	if len(result.AuthorityArtifacts) > 0 && sink == nil {
		return false, errRuntimeSink
	}
	routed := make([]AuthorityArtifact, 0, len(result.AuthorityArtifacts))
	hasCoveringReceipt := false
	for _, artifact := range result.AuthorityArtifacts {
		if len(artifact.Payload) == 0 || !validAuthorityArtifactType(artifact.Type) {
			l.setAttemptReason(refusal.Unrecognised)
			return false, errMalformedResponse
		}
		deviceID, coversEnvelope, err := validateAuthorityRouting(queued, artifact)
		if err != nil {
			l.setAttemptReason(refusal.Unrecognised)
			return false, errMalformedResponse
		}
		hasCoveringReceipt = hasCoveringReceipt || coversEnvelope
		routed = append(routed, AuthorityArtifact{
			Type: artifact.Type, DeviceID: deviceID, Payload: append([]byte(nil), artifact.Payload...),
		})
	}
	if queued.Type == ArtifactDeliveryEnvelope && !hasCoveringReceipt {
		// An acceptance that lacks the receipt it requires is not a clean
		// acceptance: an unrecognised outcome, kept for an idempotent retry that
		// can recover the correct receipt.
		l.setAttemptReason(refusal.Unrecognised)
		return false, errMalformedResponse
	}
	for _, artifact := range routed {
		if err := sink.Store(ctx, artifact); err != nil {
			// An acceptance whose returned artifacts are not durably staged
			// is not an acceptance yet: the head stays and backs off, and the
			// outcome is recorded unrecognised.
			l.setAttemptReason(refusal.Unrecognised)
			return false, errRuntimeSink
		}
	}
	if err := l.w.queue.Remove(queued.ID); err != nil {
		return false, errQueueRetirement
	}
	l.mu.Lock()
	l.attempts, l.attempted = 0, QueuedArtifact{}
	l.mu.Unlock()
	l.retired(queued)
	return true, nil
}

func (l *deliveryLane) setAttemptReason(reason string) {
	l.mu.Lock()
	l.attemptReason = reason
	l.mu.Unlock()
}

// hold records a terminal refusal of the lane's head, durably beside its queue
// record, and logs the transition into blocked once. The artifact stays queued,
// in either lane: without a verified retention a terminal refusal is held
// (evidence-transport/v2, refusal policy).
func (l *deliveryLane) hold(queued QueuedArtifact, status int, reason string) {
	record := HoldRecord{
		V: holdRecordVersion, QueueRecord: queued.ID, ArtifactDigest: payloadDigest(queued.Payload),
		ArtifactType: queued.Type, RefusalStatus: status,
		FirstHeldAtMS: time.Now().UnixMilli(),
	}
	if !heldRefusalStatus(record.RefusalStatus) {
		record.RefusalStatus = 0
	}
	record.Reason = refusal.RecordedReason(record.RefusalStatus, reason)
	// Held in memory first: a failed write must not let this process send the
	// bytes again.
	held := &heldHead{record: record, deviceID: artifactDeviceID(queued.Payload)}
	behind := l.queued() - 1
	// The hold and its failure are published together, so no status read
	// sees a held lane without its last_error.
	l.mu.Lock()
	l.held, l.stall = held, nil
	l.lastError = safeFailureReason(errChannelPermanentRefusal)
	l.lastFailureAt = time.UnixMilli(record.FirstHeldAtMS)
	l.mu.Unlock()
	persistErr := l.w.queue.Hold(record)
	l.mu.Lock()
	held.persisted = persistErr == nil
	l.mu.Unlock()
	l.w.log.Error("evidence delivery blocked: the evidence authority permanently refused the queue head",
		append(held.attrs(), "queued_behind", behind)...)
	if persistErr != nil {
		// The error text may carry a filesystem path; it is not logged.
		l.w.log.Error("evidence delivery hold was not persisted: a restart will send the refused artifact once more",
			held.attrs()...)
	}
}

// adoptedHead is the lane's head after adopting any durable hold on it.
func (l *deliveryLane) adoptedHead() (QueuedArtifact, bool) {
	queued, ok := l.head()
	if ok {
		l.adoptDurableHold(queued)
	}
	return queued, ok
}

// adoptDurableHold restores a hold persisted by an earlier process when its
// record reaches the lane's head, without sending the artifact.
func (l *deliveryLane) adoptDurableHold(queued QueuedArtifact) {
	l.mu.Lock()
	if l.held != nil && l.held.record.QueueRecord == queued.ID {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	record, ok := l.w.queue.HoldFor(queued.ID)
	if !ok {
		return
	}
	held := &heldHead{record: record, deviceID: artifactDeviceID(queued.Payload), persisted: true}
	l.mu.Lock()
	l.held, l.stall = held, nil
	l.lastError = safeFailureReason(errChannelPermanentRefusal)
	l.lastFailureAt = time.UnixMilli(record.FirstHeldAtMS)
	l.mu.Unlock()
	l.w.log.Error("evidence delivery blocked: restored the hold on a permanently refused artifact without sending it",
		append(held.attrs(), "queued_behind", l.queued()-1)...)
}

// releaseHeldUnless reports whether the lane's head is the held artifact. When
// the head has changed, the hold is released and the transition logged once.
func (l *deliveryLane) releaseHeldUnless(queued QueuedArtifact, ok bool) bool {
	l.mu.Lock()
	held := l.held
	if held != nil && ok && queued.ID == held.record.QueueRecord {
		l.mu.Unlock()
		return true
	}
	l.held = nil
	l.mu.Unlock()
	if held != nil {
		l.w.log.Warn("evidence delivery unblocked: the refused artifact is no longer at the queue head",
			append(held.attrs(), "queued", l.queued())...)
	}
	return false
}

// remind logs a held head, or a head waiting for a handoff, again. It reports
// whether either exists; neither is ever retried by the timer.
func (l *deliveryLane) remind() bool {
	l.mu.Lock()
	held := l.held
	var stall stallState
	waiting := l.stall != nil && l.stall.waitsForHandoff
	if waiting {
		l.stall.lastLogged = time.Now()
		stall = *l.stall
	}
	l.mu.Unlock()
	switch {
	case held != nil:
		since := time.UnixMilli(held.record.FirstHeldAtMS)
		l.w.log.Error("evidence delivery still blocked behind a permanently refused artifact",
			append(held.attrs(), "queued_behind", l.queued()-1,
				"blocked_for", time.Since(since).Round(time.Second).String())...)
		return true
	case waiting:
		l.w.log.Warn(stallReminderMessage, append(stall.attrs(), "queued", l.queued(),
			"stalled_for", time.Since(stall.since).Round(time.Second).String())...)
		return true
	default:
		return false
	}
}

const (
	stallMessage         = "evidence delivery stalled: the queue head was not delivered"
	stallReminderMessage = "evidence delivery still stalled at the queue head"
	resumedMessage       = "evidence delivery resumed"
)

// noteStall logs a failed attempt once on entry into a stall — the first
// failure after a success, a new head, or a newly handoff-only wait — and
// again at most once per reminder interval while it lasts.
func (l *deliveryLane) noteStall(err error) {
	now := time.Now()
	l.mu.Lock()
	head, reason := l.attempted, l.attemptReason
	waits := errors.Is(err, errChannelReceiverState)
	failure := safeFailureReason(err)
	current := l.stall
	enter := current == nil || current.id != head.ID || (waits && !current.waitsForHandoff)
	due := !enter && now.Sub(current.lastLogged) >= l.w.reminder
	if enter {
		l.stall = &stallState{
			id: head.ID, digest: payloadDigest(head.Payload), device: l.device, lane: l.lane, since: now, lastLogged: now,
		}
	}
	l.stall.failure, l.stall.reason, l.stall.waitsForHandoff = failure, reason, waits
	if due {
		l.stall.lastLogged = now
	}
	stall := *l.stall
	l.mu.Unlock()
	switch {
	case enter:
		l.w.log.Warn(stallMessage, append(stall.attrs(), "queued", l.queued())...)
	case due:
		l.w.log.Warn(stallReminderMessage, append(stall.attrs(), "queued", l.queued(),
			"stalled_for", now.Sub(stall.since).Round(time.Second).String())...)
	}
}

func (s stallState) attrs() []any {
	retry := "backoff"
	if s.waitsForHandoff {
		retry = "next_handoff"
	}
	return []any{
		"artifact_digest", s.digest, "queue_record", s.id, "device_id", s.device, "lane", string(s.lane),
		"failure", s.failure, "reason", s.reason, "retry", retry,
	}
}

func (h *heldHead) attrs() []any {
	return []any{
		"artifact_digest", h.record.ArtifactDigest, "queue_record", h.record.QueueRecord,
		"artifact_type", string(h.record.ArtifactType), "device_id", h.deviceID,
		"lane", string(LaneOf(h.record.ArtifactType)), "refusal_status", h.record.RefusalStatus, "reason", h.record.Reason,
		"first_held_at", time.UnixMilli(h.record.FirstHeldAtMS).UTC().Format(time.RFC3339),
		"acknowledged", h.record.Acknowledged, "hold_persisted", h.persisted,
	}
}

func (l *deliveryLane) recordFailure(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastFailureAt = time.Now()
	// Errors are deliberately reduced to a closed, implementation-neutral
	// vocabulary. Raw transport errors may contain hostnames or URLs.
	l.lastError = safeFailureReason(err)
}

func (l *deliveryLane) clearFailure() {
	l.mu.Lock()
	stall := l.stall
	l.lastError, l.stall = "", nil
	l.mu.Unlock()
	if stall != nil {
		l.w.log.Info(resumedMessage, append(stall.attrs(), "queued", l.queued())...)
	}
}

// Device delivery states, as gateway-evidence-carriage/v1 projects them.
const (
	DeviceHeld           = "held"
	DeviceWaitingHandoff = "waiting_handoff"
	DeviceBackingOff     = "backing_off"
)

// DeliveryStatus exposes queue and failure state without channel endpoint,
// credentials, implementation identity, or authority details.
type DeliveryStatus struct {
	Pending int
	// Degraded is true exactly when Faults or Devices is not empty.
	Degraded bool
	// Blocked is true exactly when Pending is positive, every pending
	// artifact is in a lane Devices lists, and every listed lane is held.
	Blocked       bool
	LastFailureAt time.Time
	LastError     string
	// Held is the first held lane's diagnosis, in device and lane order.
	Held *HeldStatus
	// Devices has one entry for each device and lane whose delivery is not
	// clean, in device and lane order, and none for a lane delivering
	// cleanly. No two entries share a device and lane.
	Devices []DeviceDeliveryStatus
	// Faults is the active courier-level fault set, sorted, empty when none.
	// A fault never makes Blocked true.
	Faults []string
}

// DeviceDeliveryStatus is one device's lane whose delivery is not clean.
type DeviceDeliveryStatus struct {
	DeviceID string
	Lane     Lane
	// Pending is that lane's queued artifacts.
	Pending int
	State   string
	// Held is set exactly when State is DeviceHeld.
	Held *HeldStatus
}

// HeldStatus identifies a held head. Every field is an identifier or a closed
// vocabulary value; none names an endpoint, path or credential.
type HeldStatus struct {
	QueueRecord    string
	ArtifactDigest string
	ArtifactType   ArtifactType
	RefusalStatus  int
	Reason         string
	FirstHeldAt    time.Time
	Acknowledged   bool
	Persisted      bool
}

func (w *DeliveryWorker) Status() DeliveryStatus {
	if w == nil {
		return DeliveryStatus{}
	}
	// One snapshot of the counts, so the listed lanes never sum to more than
	// the total.
	total, counts := w.queue.laneCounts()
	status := DeliveryStatus{Pending: total}
	w.mu.Lock()
	lanes := make([]*deliveryLane, 0, len(w.lanes))
	for _, l := range w.lanes {
		lanes = append(lanes, l)
	}
	w.mu.Unlock()
	sort.Slice(lanes, func(i, j int) bool {
		if lanes[i].device != lanes[j].device {
			return lanes[i].device < lanes[j].device
		}
		return lanes[i].lane < lanes[j].lane
	})
	listed := 0
	heldAll := true
	for _, l := range lanes {
		pending := counts[l.key()]
		l.mu.Lock()
		entry := DeviceDeliveryStatus{DeviceID: l.device, Lane: l.lane, Pending: pending}
		switch {
		case pending == 0:
		case l.held != nil:
			r := l.held.record
			entry.State = DeviceHeld
			entry.Held = &HeldStatus{
				QueueRecord: r.QueueRecord, ArtifactDigest: r.ArtifactDigest, ArtifactType: r.ArtifactType,
				RefusalStatus: r.RefusalStatus, Reason: r.Reason, FirstHeldAt: time.UnixMilli(r.FirstHeldAtMS),
				Acknowledged: r.Acknowledged, Persisted: l.held.persisted,
			}
		case l.stall != nil && l.stall.waitsForHandoff:
			entry.State = DeviceWaitingHandoff
		case l.lastError != "":
			entry.State = DeviceBackingOff
		}
		if l.lastError != "" && l.lastFailureAt.After(status.LastFailureAt) {
			status.LastFailureAt, status.LastError = l.lastFailureAt, l.lastError
		}
		l.mu.Unlock()
		if entry.State != "" {
			status.Devices = append(status.Devices, entry)
			listed += entry.Pending
			heldAll = heldAll && entry.State == DeviceHeld
			if entry.Held != nil && status.Held == nil {
				status.Held = entry.Held
			}
		}
	}
	// A lane with queued artifacts but no entry, including one with no lane
	// object yet, is delivering cleanly, so its artifacts are unrepresented
	// and the gateway is not blocked.
	status.Blocked = total > 0 && listed == total && heldAll
	status.Faults = w.faults.Active()
	status.Degraded = len(status.Faults) > 0 || len(status.Devices) > 0
	return status
}
