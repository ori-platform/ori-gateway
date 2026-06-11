// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package broker provides a thin MQTT transport wrapper for the gateway.
// Publish while disconnected fails immediately; messages are not queued.
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	defaultReconnectInitial = 1 * time.Second
	defaultReconnectMax     = 30 * time.Second
	defaultDisconnectMS     = 250
)

// MessageHandler receives decoded MQTT payloads. Implementations must not panic;
// the broker recovers panics and logs a warning.
type MessageHandler func(topic string, payload []byte)

// Options configures a Client.
type Options struct {
	BrokerURL string
	ClientID  string
	Logger    *slog.Logger

	// ReconnectInitial and ReconnectMax tune Paho auto-reconnect backoff.
	// Zero values use 1s initial and 30s max.
	ReconnectInitial time.Duration
	ReconnectMax     time.Duration
}

// Client wraps Eclipse Paho with explicit connection lifecycle and subscription tracking.
type Client struct {
	opts Options
	log  *slog.Logger

	mu            sync.Mutex
	connected     bool
	paho          mqtt.Client
	subscriptions map[string]subscription
}

type subscription struct {
	qos     byte
	handler MessageHandler
}

// New validates options and constructs a Client. Call Connect before Subscribe or Publish.
func New(opts Options) (*Client, error) {
	if opts.BrokerURL == "" {
		return nil, ErrEmptyBrokerURL
	}
	if opts.ClientID == "" {
		return nil, ErrEmptyClientID
	}

	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	reconnectInitial := opts.ReconnectInitial
	if reconnectInitial == 0 {
		reconnectInitial = defaultReconnectInitial
	}
	reconnectMax := opts.ReconnectMax
	if reconnectMax == 0 {
		reconnectMax = defaultReconnectMax
	}
	opts.ReconnectInitial = reconnectInitial
	opts.ReconnectMax = reconnectMax

	pahoOpts := mqtt.NewClientOptions()
	pahoOpts.AddBroker(opts.BrokerURL)
	pahoOpts.SetClientID(opts.ClientID)
	pahoOpts.SetCleanSession(true)
	pahoOpts.SetAutoReconnect(true)
	pahoOpts.SetConnectRetryInterval(reconnectInitial)
	pahoOpts.SetMaxReconnectInterval(reconnectMax)

	c := &Client{
		opts:          opts,
		log:           log,
		subscriptions: make(map[string]subscription),
	}

	pahoOpts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.mu.Lock()
		c.connected = false
		c.mu.Unlock()
		if err != nil {
			c.log.Warn("mqtt connection lost", "error", err)
		} else {
			c.log.Warn("mqtt connection lost")
		}
	})

	pahoOpts.SetOnConnectHandler(func(_ mqtt.Client) {
		c.mu.Lock()
		c.connected = true
		c.mu.Unlock()
		c.resubscribeAll()
	})

	c.paho = mqtt.NewClient(pahoOpts)
	return c, nil
}

// Connect establishes the MQTT session. It is an error to call Connect while already connected.
func (c *Client) Connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.connected {
		c.mu.Unlock()
		return ErrAlreadyConnected
	}
	c.mu.Unlock()

	token := c.paho.Connect()
	if err := waitToken(ctx, token); err != nil {
		return fmt.Errorf("broker: connect: %w", err)
	}
	if token.Error() == nil {
		c.mu.Lock()
		c.connected = true
		c.mu.Unlock()
	}
	return token.Error()
}

// Disconnect closes the MQTT session. Further Subscribe and Publish calls return ErrNotConnected.
func (c *Client) Disconnect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	if !c.connected {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	c.paho.Disconnect(defaultDisconnectMS)

	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()
	return nil
}

// Subscribe registers a handler for topic. Duplicate subscribe with the same topic and qos is a no-op.
// A second subscribe for the same topic with a different handler returns ErrSubscriptionExists.
func (c *Client) Subscribe(ctx context.Context, topic string, qos byte, handler MessageHandler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if topic == "" {
		return ErrEmptyTopic
	}
	if err := validateQoS(qos); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("broker: handler must not be nil")
	}

	c.mu.Lock()
	if !c.connected {
		c.mu.Unlock()
		return ErrNotConnected
	}
	if existing, ok := c.subscriptions[topic]; ok {
		if existing.qos == qos && handlersEqual(existing.handler, handler) {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		return ErrSubscriptionExists
	}
	c.subscriptions[topic] = subscription{qos: qos, handler: handler}
	c.mu.Unlock()

	token := c.paho.Subscribe(topic, qos, c.wrapHandler(handler))
	if err := waitToken(ctx, token); err != nil {
		c.mu.Lock()
		delete(c.subscriptions, topic)
		c.mu.Unlock()
		return fmt.Errorf("broker: subscribe: %w", err)
	}
	if token.Error() != nil {
		c.mu.Lock()
		delete(c.subscriptions, topic)
		c.mu.Unlock()
	}
	return token.Error()
}

// Publish sends payload to topic. Publish while disconnected returns ErrNotConnected immediately.
func (c *Client) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if topic == "" {
		return ErrEmptyTopic
	}
	if err := validateQoS(qos); err != nil {
		return err
	}

	c.mu.Lock()
	if !c.connected {
		c.mu.Unlock()
		return ErrNotConnected
	}
	c.mu.Unlock()

	token := c.paho.Publish(topic, qos, retain, payload)
	if err := waitToken(ctx, token); err != nil {
		return fmt.Errorf("broker: publish: %w", err)
	}
	return token.Error()
}

// IsConnected reports whether the client believes it has an active MQTT session.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

func (c *Client) wrapHandler(handler MessageHandler) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		if msg.Retained() {
			c.log.Warn("mqtt retained message rejected", "topic", msg.Topic())
			return
		}
		defer func() {
			if r := recover(); r != nil {
				c.log.Warn("mqtt message handler panic", "topic", msg.Topic(), "recover", r)
			}
		}()
		handler(msg.Topic(), msg.Payload())
	}
}

func (c *Client) resubscribeAll() {
	c.mu.Lock()
	subs := make([]struct {
		topic string
		sub   subscription
	}, 0, len(c.subscriptions))
	for topic, sub := range c.subscriptions {
		subs = append(subs, struct {
			topic string
			sub   subscription
		}{topic: topic, sub: sub})
	}
	c.mu.Unlock()

	for _, item := range subs {
		token := c.paho.Subscribe(item.topic, item.sub.qos, c.wrapHandler(item.sub.handler))
		if !token.WaitTimeout(10 * time.Second) {
			c.log.Warn("mqtt resubscribe timed out", "topic", item.topic)
			continue
		}
		if err := token.Error(); err != nil {
			c.log.Warn("mqtt resubscribe failed", "topic", item.topic, "error", err)
		}
	}
}

func waitToken(ctx context.Context, token mqtt.Token) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}

	// Paho WaitTimeout returns true when the token completed before the timeout.
	if !token.WaitTimeout(timeout) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return context.DeadlineExceeded
	}
	return token.Error()
}

func validateQoS(qos byte) error {
	if qos > 2 {
		return ErrInvalidQoS
	}
	return nil
}

func handlersEqual(a, b MessageHandler) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return fmt.Sprintf("%p", a) == fmt.Sprintf("%p", b)
}

// DefaultClientID returns a stable-ish client id for this process.
func DefaultClientID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return "ori-gateway-" + host
}
