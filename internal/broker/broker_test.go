// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqttsrv "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

func startTestBroker(t *testing.T) (brokerURL string, stop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	srv := mqttsrv.New(&mqttsrv.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "test", Address: addr})
	if err := srv.AddListener(tcp); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = srv.Serve()
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	return "tcp://" + addr, func() {
		_ = srv.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Log("mqtt test broker shutdown timed out")
		}
	}
}

func testClient(t *testing.T, brokerURL, clientID string, opts Options) *Client {
	t.Helper()
	opts.BrokerURL = brokerURL
	opts.ClientID = clientID
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Options{BrokerURL: "", ClientID: "c1"}); err != ErrEmptyBrokerURL {
		t.Fatalf("got %v", err)
	}
	if _, err := New(Options{BrokerURL: "tcp://localhost:1883", ClientID: ""}); err != ErrEmptyClientID {
		t.Fatalf("got %v", err)
	}
}

func TestConnectAndPublish(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	sub := testClient(t, brokerURL, "sub-1", Options{})
	pub := testClient(t, brokerURL, "pub-1", Options{})

	if err := sub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Disconnect(ctx)

	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Disconnect(ctx)

	const topic = "ori/test/device-a/reasoning/response"
	received := make(chan []byte, 1)
	handler := func(_ string, payload []byte) {
		received <- append([]byte(nil), payload...)
	}
	if err := sub.Subscribe(ctx, topic, QoSReasoning, handler); err != nil {
		t.Fatal(err)
	}

	want := []byte("hello-gateway")
	if err := pub.Publish(ctx, topic, QoSReasoning, false, want); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-received:
		if string(got) != string(want) {
			t.Fatalf("payload = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for published message")
	}
}

func TestSubscribeReceives(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, "recv-1", Options{})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	var count atomic.Int32
	handler := func(topic string, payload []byte) {
		if topic != "ori/gateway/health" {
			t.Errorf("unexpected topic %q", topic)
		}
		if string(payload) != "ok" {
			t.Errorf("unexpected payload %q", payload)
		}
		count.Add(1)
	}
	if err := c.Subscribe(ctx, "ori/gateway/health", QoSHeartbeat, handler); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, "ori/gateway/health", QoSHeartbeat, false, []byte("ok")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for count.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if count.Load() == 0 {
		t.Fatal("handler was not invoked")
	}
}

func TestDisconnectClean(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, "disc-1", Options{})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if c.IsConnected() {
		t.Fatal("expected disconnected")
	}
	if err := c.Publish(ctx, "ori/gateway/health", QoSHeartbeat, false, nil); err != ErrNotConnected {
		t.Fatalf("publish err = %v, want ErrNotConnected", err)
	}
	if err := c.Subscribe(ctx, "ori/gateway/health", QoSHeartbeat, func(string, []byte) {}); err != ErrNotConnected {
		t.Fatalf("subscribe err = %v, want ErrNotConnected", err)
	}
}

func TestSubscribeBeforeConnect(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	c := testClient(t, brokerURL, "pre-1", Options{})
	err := c.Subscribe(context.Background(), "ori/gateway/health", QoSHeartbeat, func(string, []byte) {})
	if err != ErrNotConnected {
		t.Fatalf("got %v, want ErrNotConnected", err)
	}
}

func TestPublishWhileDisconnected(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	c := testClient(t, brokerURL, "pub-offline", Options{})
	err := c.Publish(context.Background(), "ori/gateway/health", QoSHeartbeat, false, []byte("x"))
	if err != ErrNotConnected {
		t.Fatalf("got %v, want ErrNotConnected", err)
	}
}

func TestDuplicateSubscribeIdempotent(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, "dup-1", Options{})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	handler := func(string, []byte) {}
	topic := "ori/test/device-b/reasoning/request"
	if err := c.Subscribe(ctx, topic, QoSReasoning, handler); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe(ctx, topic, QoSReasoning, handler); err != nil {
		t.Fatalf("duplicate subscribe should be no-op: %v", err)
	}

	other := func(string, []byte) {}
	if err := c.Subscribe(ctx, topic, QoSReasoning, other); err != ErrSubscriptionExists {
		t.Fatalf("got %v, want ErrSubscriptionExists", err)
	}
}

func TestHandlerPanicDoesNotBreakClient(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, "panic-1", Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	topic := "ori/test/panic-topic"
	if err := c.Subscribe(ctx, topic, QoSReasoning, func(string, []byte) {
		panic("boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(ctx, topic, QoSReasoning, false, []byte("1")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := c.Publish(ctx, topic, QoSReasoning, false, []byte("2")); err != nil {
		t.Fatalf("publish after handler panic failed: %v", err)
	}
}

func TestReconnectBackoffOptions(t *testing.T) {
	c, err := New(Options{BrokerURL: "tcp://127.0.0.1:1883", ClientID: "default-backoff"})
	if err != nil {
		t.Fatal(err)
	}
	if c.opts.ReconnectInitial != defaultReconnectInitial {
		t.Fatalf("default reconnect initial = %s, want %s", c.opts.ReconnectInitial, defaultReconnectInitial)
	}
	if c.opts.ReconnectMax != defaultReconnectMax {
		t.Fatalf("default reconnect max = %s, want %s", c.opts.ReconnectMax, defaultReconnectMax)
	}

	c, err = New(Options{
		BrokerURL:        "tcp://127.0.0.1:1883",
		ClientID:         "custom-backoff",
		ReconnectInitial: 200 * time.Millisecond,
		ReconnectMax:     500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.opts.ReconnectInitial != 200*time.Millisecond {
		t.Fatalf("custom reconnect initial = %s", c.opts.ReconnectInitial)
	}
	if c.opts.ReconnectMax != 500*time.Millisecond {
		t.Fatalf("custom reconnect max = %s", c.opts.ReconnectMax)
	}
}

func TestReconnectIntegration(t *testing.T) {
	if os.Getenv("ORI_GATEWAY_MQTT_RECONNECT_INTEGRATION") == "" {
		t.Skip("set ORI_GATEWAY_MQTT_RECONNECT_INTEGRATION=1 to run broker restart reconnect integration test")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	brokerURL := "tcp://" + addr

	start := func() (*mqttsrv.Server, <-chan struct{}) {
		srv := mqttsrv.New(&mqttsrv.Options{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
			t.Fatal(err)
		}
		tcp := listeners.NewTCP(listeners.Config{ID: "test", Address: addr})
		if err := srv.AddListener(tcp); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() {
			_ = srv.Serve()
			close(done)
		}()
		waitForTCP(t, addr)
		return srv, done
	}

	srv1, srv1Done := start()

	ctx := context.Background()
	c := testClient(t, brokerURL, "reconn-1", Options{
		ReconnectInitial: 200 * time.Millisecond,
		ReconnectMax:     500 * time.Millisecond,
	})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	if err := c.Publish(ctx, "ori/gateway/health", QoSHeartbeat, false, []byte("before")); err != nil {
		t.Fatal(err)
	}

	_ = srv1.Close()
	waitServerStopped(t, srv1Done)
	waitDisconnected(t, c, 3*time.Second)

	srv2, srv2Done := start()
	defer func() {
		_ = srv2.Close()
		waitServerStopped(t, srv2Done)
	}()

	deadline := time.Now().Add(30 * time.Second)
	var published atomic.Bool
	for time.Now().Before(deadline) {
		if err := c.Publish(ctx, "ori/gateway/health", QoSHeartbeat, false, []byte("after")); err == nil {
			published.Store(true)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !published.Load() {
		t.Fatal("publish after reconnect never succeeded")
	}
	if !c.IsConnected() {
		t.Fatal("client publish succeeded after reconnect but connected flag was not restored")
	}
}

func TestConnectAlreadyConnected(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, "twice-1", Options{})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	if err := c.Connect(ctx); err != ErrAlreadyConnected {
		t.Fatalf("got %v, want ErrAlreadyConnected", err)
	}
}

func TestQoSConstants(t *testing.T) {
	if QoSHeartbeat != 0 || QoSReasoning != 1 {
		t.Fatalf("unexpected qos constants: heartbeat=%d reasoning=%d", QoSHeartbeat, QoSReasoning)
	}
}

func waitDisconnected(t *testing.T, c *Client, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !c.IsConnected() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("expected disconnected")
}

func waitServerStopped(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("mqtt test broker shutdown timed out")
	}
}

func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("broker at %s did not become ready", addr)
}

func TestDefaultClientID(t *testing.T) {
	id := DefaultClientID()
	if id == "" || len(id) < len("ori-gateway-") {
		t.Fatalf("unexpected client id: %q", id)
	}
}

func TestInvalidQoS(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, "qos-1", Options{})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	if err := c.Publish(ctx, "t", 3, false, nil); err != ErrInvalidQoS {
		t.Fatalf("publish: got %v", err)
	}
}

func TestConcurrentSubscribePublish(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, fmt.Sprintf("conc-%d", time.Now().UnixNano()), Options{})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	topic := "ori/test/concurrent"
	var wg sync.WaitGroup
	handler := func(string, []byte) { wg.Done() }
	if err := c.Subscribe(ctx, topic, QoSReasoning, handler); err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	if err := c.Publish(ctx, topic, QoSReasoning, false, []byte("x")); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for handler")
	}
}

// stubToken implements mqtt.Token for waitToken unit tests.
type stubToken struct {
	waitOK bool
	err    error
	done   chan struct{}
}

func (s stubToken) Wait() bool { return s.waitOK }

func (s stubToken) WaitTimeout(_ time.Duration) bool { return s.waitOK }

func (s stubToken) Done() <-chan struct{} {
	if s.done == nil {
		s.done = make(chan struct{})
		if s.waitOK {
			close(s.done)
		}
	}
	return s.done
}

func (s stubToken) Error() error { return s.err }

type slowToken struct {
	release chan struct{}
}

func newSlowToken() slowToken {
	return slowToken{release: make(chan struct{})}
}

func (s slowToken) Wait() bool {
	<-s.release
	return true
}

func (s slowToken) WaitTimeout(d time.Duration) bool {
	select {
	case <-s.release:
		return true
	case <-time.After(d):
		return false
	}
}

func (s slowToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		<-s.release
		close(ch)
	}()
	return ch
}

func (s slowToken) Error() error { return nil }

func TestWaitTokenCompleted(t *testing.T) {
	ctx := context.Background()
	if err := waitToken(ctx, stubToken{waitOK: true}); err != nil {
		t.Fatalf("waitToken: %v", err)
	}
}

func TestWaitTokenReturnsTokenError(t *testing.T) {
	want := errors.New("mqtt refused")
	err := waitToken(context.Background(), stubToken{waitOK: true, err: want})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func TestWaitTokenDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := waitToken(ctx, newSlowToken())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestWaitTokenRespectsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitToken(ctx, stubToken{waitOK: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want %v", err, context.Canceled)
	}
}

func TestConnectRespectsDeadline(t *testing.T) {
	// TEST-NET address with no broker; connect should not finish before ctx expires.
	c, err := New(Options{
		BrokerURL: "tcp://192.0.2.1:1883",
		ClientID:  "deadline-connect",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = c.Connect(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Connect: got %v, want %v", err, context.DeadlineExceeded)
	}
	if c.IsConnected() {
		t.Fatal("client must not be connected after connect deadline")
	}
}

func TestPublishRespectsDeadline(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, "pub-deadline", Options{})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	pubCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Nanosecond)

	err := c.Publish(pubCtx, "ori/gateway/health", QoSHeartbeat, false, []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish: got %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestSubscribeRespectsDeadline(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx := context.Background()
	c := testClient(t, brokerURL, "sub-deadline", Options{})
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(ctx)

	subCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Nanosecond)

	err := c.Subscribe(subCtx, "ori/gateway/health", QoSHeartbeat, func(string, []byte) {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Subscribe: got %v, want %v", err, context.DeadlineExceeded)
	}
}

type retainedTestMessage struct {
	topic    string
	payload  []byte
	retained bool
}

func (m retainedTestMessage) Duplicate() bool   { return false }
func (m retainedTestMessage) Qos() byte         { return QoSHeartbeat }
func (m retainedTestMessage) Retained() bool    { return m.retained }
func (m retainedTestMessage) Topic() string     { return m.topic }
func (m retainedTestMessage) MessageID() uint16 { return 1 }
func (m retainedTestMessage) Payload() []byte   { return m.payload }
func (m retainedTestMessage) Ack()              {}

func TestWrapHandlerRejectsRetainedMessages(t *testing.T) {
	c, err := New(Options{
		BrokerURL: "tcp://localhost:1883",
		ClientID:  "test-retained",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	handler := c.wrapHandler(func(string, []byte) {
		called = true
	})
	handler(nil, retainedTestMessage{topic: "ori/dev-01/runtime/heartbeat", payload: []byte("stale"), retained: true})
	if called {
		t.Fatal("retained message should not reach application handler")
	}
}
