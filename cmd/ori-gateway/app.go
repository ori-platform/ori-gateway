// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/dispatcher"
	"github.com/ori-platform/ori-gateway/internal/enrichment"
	"github.com/ori-platform/ori-gateway/internal/fleet"
	"github.com/ori-platform/ori-gateway/internal/heartbeat"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/provider"
	"github.com/ori-platform/ori-gateway/internal/sim"
	"github.com/ori-platform/ori-gateway/internal/site"
	"github.com/ori-platform/ori-gateway/internal/webhookbridge"
)

const (
	defaultConfigPath          = "gateway.yaml"
	defaultClientID            = "ori-gateway"
	defaultRequestFailureLimit = 5
)

type brokerClient interface {
	Connect(ctx context.Context) error
	Disconnect(ctx context.Context) error
	Subscribe(ctx context.Context, topic string, qos byte, handler broker.MessageHandler) error
	Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error
}

type heartbeatRunner interface {
	Run(ctx context.Context) error
}

type webhookBridgeRunner interface {
	Run(ctx context.Context) error
}

type appDependencies struct {
	loadConfig                 func(path string) (config.Config, error)
	newProvider                func(cfg config.ProviderConfig) (provider.Provider, error)
	newTierCEnrichmentProvider func(cfg config.ReportingConfig) (enrichment.Provider, error)
	newBroker                  func(opts broker.Options) (brokerClient, error)
	newSIM                     func(cfg config.SIMConfig, opts sim.Options) (*sim.Client, error)
	newFleet                   func(cfg config.FleetConfig, opts fleet.Options) (*fleet.Client, error)
	newWebhookBridge           func(cfg config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error)
	newSiteRegistry            func() *site.Registry
	newHeartbeat               func(
		publish heartbeat.PublishFunc,
		prov heartbeat.ProviderStatus,
		simStatus heartbeat.SIMStatus,
		opts heartbeat.Options,
	) (heartbeatRunner, error)
	logger *slog.Logger
	now    func() time.Time
}

func defaultDependencies() appDependencies {
	return appDependencies{
		loadConfig:  config.Load,
		newProvider: provider.NewFromConfig,
		newTierCEnrichmentProvider: func(config.ReportingConfig) (enrichment.Provider, error) {
			return nil, fmt.Errorf("tier C enrichment reporting provider is not wired")
		},
		newBroker: func(opts broker.Options) (brokerClient, error) {
			return broker.New(opts)
		},
		newSIM:   sim.New,
		newFleet: fleet.New,
		newWebhookBridge: func(cfg config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error) {
			return webhookbridge.New(cfg, opts)
		},
		newSiteRegistry: site.NewRegistry,
		newHeartbeat: func(
			publish heartbeat.PublishFunc,
			prov heartbeat.ProviderStatus,
			simStatus heartbeat.SIMStatus,
			opts heartbeat.Options,
		) (heartbeatRunner, error) {
			return heartbeat.NewPublisher(publish, prov, simStatus, opts)
		},
		logger: slog.Default(),
		now:    time.Now,
	}
}

func runCLI(args []string, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("ori-gateway", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultConfigPath, "path to gateway.yaml")
	if err := flags.Parse(args); err != nil {
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	deps := defaultDependencies()
	if err := runGateway(ctx, *configPath, deps); err != nil {
		_, _ = fmt.Fprintf(stderr, "ori-gateway: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintln(stdout, "ori-gateway stopped")
	return 0
}

func runGateway(ctx context.Context, configPath string, deps appDependencies) error {
	deps = normalizeDependencies(deps)

	cfg, err := deps.loadConfig(configPath)
	if err != nil {
		return err
	}

	reasoningProvider, err := deps.newProvider(cfg.Provider)
	if err != nil {
		return fmt.Errorf("construct provider: %w", err)
	}
	providerStatus := providerStatusFromProvider(reasoningProvider)

	var webhookBridge webhookBridgeRunner
	if cfg.WebhookBridge.Enabled {
		webhookBridge, err = deps.newWebhookBridge(cfg.WebhookBridge, webhookbridge.Options{
			Logger: deps.logger,
			Now:    deps.now,
		})
		if err != nil {
			return fmt.Errorf("construct webhook bridge: %w", err)
		}
	}

	client, err := deps.newBroker(broker.Options{
		BrokerURL: cfg.Gateway.BrokerURL,
		ClientID:  defaultClientID,
		Logger:    deps.logger,
	})
	if err != nil {
		return fmt.Errorf("construct broker: %w", err)
	}
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect broker: %w", err)
	}
	disconnect := true
	defer func() {
		if disconnect {
			_ = client.Disconnect(context.Background())
		}
	}()

	simClient, err := deps.newSIM(cfg.SIM, sim.Options{})
	if err != nil {
		return fmt.Errorf("construct sim: %w", err)
	}
	fleetClient, err := deps.newFleet(cfg.Fleet, fleet.Options{})
	if err != nil {
		return fmt.Errorf("construct fleet: %w", err)
	}
	// Fleet status is constructed here so startup validates the optional-module
	// boundary. It is intentionally not published until the heartbeat contract
	// includes fleet health fields.
	_ = fleetClient

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var webhookBridgeErr <-chan error
	if webhookBridge != nil {
		ch := make(chan error, 1)
		webhookBridgeErr = ch
		go func() {
			if err := webhookBridge.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				ch <- err
				return
			}
			ch <- nil
		}()
	}

	heartbeatAuth, err := heartbeatAuthFromConfig(cfg.Gateway.Auth)
	if err != nil {
		cancel()
		_ = waitOptionalRunner(webhookBridgeErr)
		return err
	}

	siteRegistry := deps.newSiteRegistry()
	runtimeHeartbeatHandler, err := runtimeHeartbeatHandlerFromConfig(cfg.Gateway.Auth, heartbeatAuth.SharedSecret, siteRegistry, deps.now)
	if err != nil {
		cancel()
		_ = waitOptionalRunner(webhookBridgeErr)
		return err
	}

	hb, err := deps.newHeartbeat(
		func(ctx context.Context, payload []byte) error {
			return client.Publish(ctx, contracts.GatewayHealthTopic, broker.QoSHeartbeat, false, payload)
		},
		providerStatus,
		heartbeat.SIMStatus{
			Enabled: cfg.SIM.Enabled,
			Probe: func() bool {
				return simClient.Available(runCtx)
			},
		},
		heartbeat.Options{
			Interval:  time.Duration(cfg.Gateway.HeartbeatIntervalS) * time.Second,
			StartedAt: deps.now(),
			Now:       deps.now,
			Logger:    deps.logger,
			Auth:      heartbeatAuth,
		},
	)
	if err != nil {
		cancel()
		_ = waitOptionalRunner(webhookBridgeErr)
		return fmt.Errorf("construct heartbeat: %w", err)
	}

	heartbeatErr := make(chan error, 1)
	go func() {
		if err := hb.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			heartbeatErr <- err
			return
		}
		heartbeatErr <- nil
	}()

	go evictStaleRuntimeNodes(runCtx, siteRegistry, time.Duration(cfg.Gateway.HeartbeatIntervalS)*time.Second, deps.now, deps.logger)

	reasoningDispatcher, err := dispatcher.New(client, reasoningProvider, dispatcher.Options{
		ProviderTimeoutMS: cfg.Provider.TimeoutMS,
	})
	if err != nil {
		cancel()
		<-heartbeatErr
		_ = waitOptionalRunner(webhookBridgeErr)
		return fmt.Errorf("construct dispatcher: %w", err)
	}

	requestErr := make(chan error, 1)
	var requestFailureMu sync.Mutex
	requestFailures := 0
	if err := client.Subscribe(runCtx, contracts.GatewayReasoningRequestTopicFilter, broker.QoSReasoning, func(topic string, payload []byte) {
		if err := reasoningDispatcher.HandleRequest(runCtx, topic, payload); err != nil {
			deps.logger.Warn("reasoning request handling failed", "topic", topic, "error", err)
			requestFailureMu.Lock()
			requestFailures++
			failures := requestFailures
			requestFailureMu.Unlock()
			if failures >= defaultRequestFailureLimit {
				select {
				case requestErr <- fmt.Errorf("reasoning request failure limit reached: %w", err):
				default:
				}
			}
			return
		}
		requestFailureMu.Lock()
		requestFailures = 0
		requestFailureMu.Unlock()
	}); err != nil {
		cancel()
		<-heartbeatErr
		_ = waitOptionalRunner(webhookBridgeErr)
		return fmt.Errorf("subscribe reasoning requests: %w", err)
	}

	for _, deviceID := range cfg.Gateway.DeviceIDs {
		topic, err := contracts.RuntimeNodeHeartbeatTopic(deviceID)
		if err != nil {
			cancel()
			<-heartbeatErr
			_ = waitOptionalRunner(webhookBridgeErr)
			return fmt.Errorf("runtime node heartbeat topic: %w", err)
		}
		if err := client.Subscribe(runCtx, topic, broker.QoSHeartbeat, func(topic string, payload []byte) {
			if err := runtimeHeartbeatHandler.Handle(topic, payload); err != nil {
				deps.logger.Warn("runtime node heartbeat rejected", "topic", topic, "error", err)
			}
		}); err != nil {
			cancel()
			<-heartbeatErr
			_ = waitOptionalRunner(webhookBridgeErr)
			return fmt.Errorf("subscribe runtime node heartbeats: %w", err)
		}
	}

	if cfg.Reporting.TierCEnrichment.Enabled {
		enrichmentProvider, err := deps.newTierCEnrichmentProvider(cfg.Reporting)
		if err != nil {
			cancel()
			<-heartbeatErr
			_ = waitOptionalRunner(webhookBridgeErr)
			return fmt.Errorf("construct tier c enrichment provider: %w", err)
		}
		enrichmentHandler, err := enrichment.NewHandler(client, enrichmentProvider, enrichment.Options{Logger: deps.logger})
		if err != nil {
			cancel()
			<-heartbeatErr
			_ = waitOptionalRunner(webhookBridgeErr)
			return fmt.Errorf("construct tier c enrichment handler: %w", err)
		}
		for _, deviceID := range cfg.Gateway.DeviceIDs {
			topic, err := contracts.TierCEnrichmentRequestTopic(deviceID)
			if err != nil {
				cancel()
				<-heartbeatErr
				_ = waitOptionalRunner(webhookBridgeErr)
				return fmt.Errorf("tier c enrichment request topic: %w", err)
			}
			if err := client.Subscribe(runCtx, topic, broker.QoSReasoning, func(topic string, payload []byte) {
				if err := enrichmentHandler.HandleRequest(runCtx, topic, payload); err != nil {
					deps.logger.Warn("tier c enrichment request handling failed", "topic", topic, "error", err)
				}
			}); err != nil {
				cancel()
				<-heartbeatErr
				_ = waitOptionalRunner(webhookBridgeErr)
				return fmt.Errorf("subscribe tier c enrichment requests: %w", err)
			}
		}
	}

	select {
	case <-ctx.Done():
		cancel()
		<-heartbeatErr
		_ = waitOptionalRunner(webhookBridgeErr)
		err := client.Disconnect(context.Background())
		disconnect = false
		if err != nil {
			return fmt.Errorf("disconnect broker: %w", err)
		}
		return nil
	case err := <-heartbeatErr:
		cancel()
		_ = waitOptionalRunner(webhookBridgeErr)
		if err != nil {
			return fmt.Errorf("heartbeat stopped: %w", err)
		}
		return fmt.Errorf("heartbeat stopped unexpectedly")
	case err := <-webhookBridgeErr:
		cancel()
		<-heartbeatErr
		if err != nil {
			return fmt.Errorf("webhook bridge stopped: %w", err)
		}
		return fmt.Errorf("webhook bridge stopped unexpectedly")
	case err := <-requestErr:
		cancel()
		<-heartbeatErr
		_ = waitOptionalRunner(webhookBridgeErr)
		return err
	}
}

func waitOptionalRunner(ch <-chan error) error {
	if ch == nil {
		return nil
	}
	return <-ch
}

func evictStaleRuntimeNodes(
	ctx context.Context,
	registry *site.Registry,
	interval time.Duration,
	now func() time.Time,
	logger *slog.Logger,
) {
	if registry == nil {
		return
	}
	if interval <= 0 {
		interval = time.Duration(config.DefaultHeartbeatIntervalS) * time.Second
	}
	if now == nil {
		now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	ttlMS := (3 * interval).Milliseconds()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			evicted := registry.EvictStale(now().UnixMilli(), ttlMS)
			for _, node := range evicted {
				logger.Warn("runtime node heartbeat stale", "device_id", node.DeviceID, "last_seen_ms", node.LastSeenMS)
			}
		}
	}
}

func heartbeatAuthFromConfig(auth config.GatewayAuthConfig) (heartbeat.AuthConfig, error) {
	secret, enabled, err := gatewayAuthSecretFromConfig(auth)
	if err != nil {
		return heartbeat.AuthConfig{}, err
	}
	if !enabled {
		return heartbeat.AuthConfig{}, nil
	}
	return heartbeat.AuthConfig{Enabled: true, SharedSecret: secret}, nil
}

func gatewayAuthSecretFromConfig(auth config.GatewayAuthConfig) (string, bool, error) {
	if !auth.Enabled {
		return "", false, nil
	}
	envName := strings.TrimSpace(auth.SharedSecretEnv)
	if envName == "" {
		envName = config.DefaultGatewayAuthSecretEnv
	}
	secret := strings.TrimSpace(os.Getenv(envName))
	if secret == "" {
		return "", false, fmt.Errorf("gateway.auth.enabled is true but environment variable %q is empty", envName)
	}
	return secret, true, nil
}

func runtimeHeartbeatHandlerFromConfig(
	auth config.GatewayAuthConfig,
	sharedSecret string,
	registry *site.Registry,
	now func() time.Time,
) (*site.RuntimeHeartbeatHandler, error) {
	var verifier *mqttauth.Verifier
	if auth.Enabled {
		var err error
		verifier, err = mqttauth.NewVerifier(mqttauth.Config{
			SharedSecret: sharedSecret,
			Now:          now,
		})
		if err != nil {
			return nil, err
		}
	}
	return site.NewRuntimeHeartbeatHandler(registry, site.RuntimeHeartbeatHandlerOptions{
		AuthVerifier: verifier,
		Now:          now,
	})
}

func normalizeDependencies(deps appDependencies) appDependencies {
	defaults := defaultDependencies()
	if deps.loadConfig == nil {
		deps.loadConfig = defaults.loadConfig
	}
	if deps.newProvider == nil {
		deps.newProvider = defaults.newProvider
	}
	if deps.newTierCEnrichmentProvider == nil {
		deps.newTierCEnrichmentProvider = defaults.newTierCEnrichmentProvider
	}
	if deps.newBroker == nil {
		deps.newBroker = defaults.newBroker
	}
	if deps.newSIM == nil {
		deps.newSIM = defaults.newSIM
	}
	if deps.newFleet == nil {
		deps.newFleet = defaults.newFleet
	}
	if deps.newWebhookBridge == nil {
		deps.newWebhookBridge = defaults.newWebhookBridge
	}
	if deps.newSiteRegistry == nil {
		deps.newSiteRegistry = defaults.newSiteRegistry
	}
	if deps.newHeartbeat == nil {
		deps.newHeartbeat = defaults.newHeartbeat
	}
	if deps.logger == nil {
		deps.logger = defaults.logger
	}
	if deps.now == nil {
		deps.now = defaults.now
	}
	return deps
}

type staticProviderStatus struct {
	name    string
	healthy func(ctx context.Context) bool
}

func (s staticProviderStatus) Name() string {
	return s.name
}

func (s staticProviderStatus) Healthy(ctx context.Context) bool {
	if s.healthy == nil {
		return false
	}
	return s.healthy(ctx)
}

func providerStatusFromProvider(reasoningProvider provider.Provider) heartbeat.ProviderStatus {
	if status, ok := reasoningProvider.(heartbeat.ProviderStatus); ok {
		return status
	}
	return staticProviderStatus{
		name: reasoningProvider.Name(),
		healthy: func(context.Context) bool {
			return false
		},
	}
}
