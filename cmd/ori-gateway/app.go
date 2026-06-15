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
	"sync/atomic"
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
	"github.com/ori-platform/ori-gateway/internal/reporting"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
	"github.com/ori-platform/ori-gateway/internal/sim"
	"github.com/ori-platform/ori-gateway/internal/site"
	"github.com/ori-platform/ori-gateway/internal/webhookbridge"
	"golang.org/x/sync/errgroup"
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

type weeklyReportRunner interface {
	Run(ctx context.Context) error
}

type appDependencies struct {
	loadConfig                 func(path string) (config.Config, error)
	newProvider                func(cfg config.ProviderConfig) (provider.Provider, error)
	newTierCEnrichmentProvider func(cfg config.ReportingConfig) (enrichment.Provider, error)
	newReportingProvider       func(cfg config.ReportingConfig) (reporting.Provider, error)
	newBroker                  func(opts broker.Options) (brokerClient, error)
	newSIM                     func(cfg config.SIMConfig, opts sim.Options) (*sim.Client, error)
	newFleet                   func(cfg config.FleetConfig, opts fleet.Options) (*fleet.Client, error)
	newWebhookBridge           func(cfg config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error)
	newRuntimeClient           func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error)
	newWeeklyReportRunner      func(generator *reporting.WeeklyReportGenerator, req reporting.WeeklyReportRequest, schedule reporting.Schedule, opts reporting.RunnerOptions) (weeklyReportRunner, error)
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
		newReportingProvider: func(cfg config.ReportingConfig) (reporting.Provider, error) {
			return reporting.NewProviderFromConfig(cfg, reporting.ProviderOptions{})
		},
		newBroker: func(opts broker.Options) (brokerClient, error) {
			return broker.New(opts)
		},
		newSIM:   sim.New,
		newFleet: fleet.New,
		newWebhookBridge: func(cfg config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error) {
			return webhookbridge.New(cfg, opts)
		},
		newRuntimeClient: func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
			return runtimeclient.NewMQTTClient(b, opts...)
		},
		newWeeklyReportRunner: func(generator *reporting.WeeklyReportGenerator, req reporting.WeeklyReportRequest, schedule reporting.Schedule, opts reporting.RunnerOptions) (weeklyReportRunner, error) {
			return reporting.NewWeeklyReportRunner(generator, req, schedule, opts)
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
	startedAt := deps.now()

	cfg, err := deps.loadConfig(configPath)
	if err != nil {
		return err
	}
	gatewaySecrets, err := gatewayAuthSecretsFromConfig(cfg.Gateway.Auth)
	if err != nil {
		return err
	}

	reasoningProvider, err := deps.newProvider(cfg.Provider)
	if err != nil {
		return fmt.Errorf("construct provider: %w", err)
	}
	providerStatus := providerStatusFromProvider(reasoningProvider)

	var webhookBridge webhookBridgeRunner
	var webhookBridgeReady atomic.Bool
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

	var weeklyReport weeklyReportRunner
	if cfg.Reporting.WeeklyReport.Enabled {
		runtimeClientOptions, err := runtimeClientOptionsFromSecrets(cfg.Gateway.Encryption, gatewaySecrets, deps.now)
		if err != nil {
			return fmt.Errorf("construct runtime export client options: %w", err)
		}
		runtimeExportClient, err := deps.newRuntimeClient(client, runtimeClientOptions...)
		if err != nil {
			return fmt.Errorf("construct runtime export client: %w", err)
		}
		reportingProvider, err := deps.newReportingProvider(cfg.Reporting)
		if err != nil {
			return fmt.Errorf("construct reporting provider: %w", err)
		}
		weeklyGenerator, err := reporting.NewWeeklyReportGenerator(runtimeExportClient, reportingProvider, reporting.WithNow(deps.now))
		if err != nil {
			return fmt.Errorf("construct weekly report generator: %w", err)
		}
		weeklySchedule, err := reporting.NewSchedule(cfg.Reporting.WeeklyReport.Day, cfg.Reporting.WeeklyReport.Time, cfg.Reporting.WeeklyReport.Timezone)
		if err != nil {
			return fmt.Errorf("construct weekly report schedule: %w", err)
		}
		deliverers := []reporting.Deliverer{&reporting.LogDeliverer{Logger: deps.logger}}
		if cfg.Reporting.WeeklyReport.Delivery.File.Enabled {
			fd, err := reporting.NewFileDeliverer(cfg.Reporting.WeeklyReport.Delivery.File.Path)
			if err != nil {
				return fmt.Errorf("weekly report file deliverer: %w", err)
			}
			deliverers = append(deliverers, fd)
		}
		if cfg.Reporting.WeeklyReport.Delivery.Cloud.Enabled {
			return fmt.Errorf("weekly report cloud delivery is not yet implemented")
		}
		weeklyReport, err = deps.newWeeklyReportRunner(
			weeklyGenerator,
			weeklyReportRequestFromConfig(cfg.Reporting.WeeklyReport),
			weeklySchedule,
			reporting.RunnerOptions{Logger: deps.logger, Now: deps.now, Deliverers: deliverers},
		)
		if err != nil {
			return fmt.Errorf("construct weekly report runner: %w", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runners := newSupervisedRunners(runCtx)
	shutdownRunners := func() {
		cancel()
		_ = runners.wait()
	}

	if weeklyReport != nil {
		runners.start("weekly report runner", weeklyReport.Run)
	}
	if webhookBridge != nil {
		webhookBridgeReady.Store(true)
		runners.start("webhook bridge", func(ctx context.Context) error {
			defer webhookBridgeReady.Store(false)
			return webhookBridge.Run(ctx)
		})
	}

	heartbeatAuth := heartbeatAuthFromSecrets(gatewaySecrets)

	siteRegistry := deps.newSiteRegistry()
	runtimeHeartbeatHandler, err := runtimeHeartbeatHandlerFromSecrets(gatewaySecrets, siteRegistry, deps.now)
	if err != nil {
		shutdownRunners()
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
			Interval:      time.Duration(cfg.Gateway.HeartbeatIntervalS) * time.Second,
			StartedAt:     deps.now(),
			Now:           deps.now,
			Logger:        deps.logger,
			Auth:          heartbeatAuth,
			WebhookBridge: webhookBridgePostureProbe(cfg.WebhookBridge, webhookBridgeReady.Load),
		},
	)
	if err != nil {
		shutdownRunners()
		return fmt.Errorf("construct heartbeat: %w", err)
	}
	runners.start("heartbeat", hb.Run)

	go evictStaleRuntimeNodes(runCtx, siteRegistry, time.Duration(cfg.Gateway.HeartbeatIntervalS)*time.Second, deps.now, deps.logger)

	reasoningDispatcher, err := dispatcher.New(client, reasoningProvider, dispatcher.Options{
		ProviderTimeoutMS: cfg.Provider.TimeoutMS,
	})
	if err != nil {
		shutdownRunners()
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
		shutdownRunners()
		return fmt.Errorf("subscribe reasoning requests: %w", err)
	}

	for _, deviceID := range cfg.Gateway.DeviceIDs {
		topic, err := contracts.RuntimeNodeHeartbeatTopic(deviceID)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("runtime node heartbeat topic: %w", err)
		}
		if err := client.Subscribe(runCtx, topic, broker.QoSHeartbeat, func(topic string, payload []byte) {
			if err := runtimeHeartbeatHandler.Handle(topic, payload); err != nil {
				deps.logger.Warn("runtime node heartbeat rejected", "topic", topic, "error", err)
			}
		}); err != nil {
			shutdownRunners()
			return fmt.Errorf("subscribe runtime node heartbeats: %w", err)
		}
	}

	if cfg.Reporting.TierCEnrichment.Enabled {
		enrichmentProvider, err := deps.newTierCEnrichmentProvider(cfg.Reporting)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("construct tier c enrichment provider: %w", err)
		}
		enrichmentHandler, err := enrichment.NewHandler(client, enrichmentProvider, enrichment.Options{Logger: deps.logger})
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("construct tier c enrichment handler: %w", err)
		}
		for _, deviceID := range cfg.Gateway.DeviceIDs {
			topic, err := contracts.TierCEnrichmentRequestTopic(deviceID)
			if err != nil {
				shutdownRunners()
				return fmt.Errorf("tier c enrichment request topic: %w", err)
			}
			if err := client.Subscribe(runCtx, topic, broker.QoSReasoning, func(topic string, payload []byte) {
				if err := enrichmentHandler.HandleRequest(runCtx, topic, payload); err != nil {
					deps.logger.Warn("tier c enrichment request handling failed", "topic", topic, "error", err)
				}
			}); err != nil {
				shutdownRunners()
				return fmt.Errorf("subscribe tier c enrichment requests: %w", err)
			}
		}
	}

	if cfg.SiteHealth.Enabled {
		siteProjector := site.NewProjector(siteRegistry, site.ProjectOptions{
			ExpectedDeviceIDs: cfg.Gateway.DeviceIDs,
			NodeTTLMS:         (3 * time.Duration(cfg.Gateway.HeartbeatIntervalS) * time.Second).Milliseconds(),
		})
		gatewayViewFn := func() site.GatewayView {
			status := site.SiteStatusHealthy
			if !providerStatus.Healthy(runCtx) {
				status = site.SiteStatusDegraded
			}
			return site.GatewayView{
				Status:               status,
				ProviderName:         providerStatus.Name(),
				UptimeS:              deps.now().Sub(startedAt).Seconds(),
				WebhookBridgeEnabled: cfg.WebhookBridge.Enabled,
				WebhookBridgeReady:   webhookBridgeReady.Load(),
			}
		}
		runners.start("site health server", site.NewHealthHandler(
			siteProjector, gatewayViewFn, cfg.SiteHealth.ListenAddr, deps.now,
		).Run)
	}

	select {
	case <-ctx.Done():
		shutdownRunners()
		return disconnectBroker(client, &disconnect)
	case err := <-runners.done():
		cancel()
		if ctx.Err() != nil {
			return disconnectBroker(client, &disconnect)
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("runner group stopped unexpectedly")
	case err := <-requestErr:
		shutdownRunners()
		return err
	}
}

func disconnectBroker(client brokerClient, disconnect *bool) error {
	err := client.Disconnect(context.Background())
	*disconnect = false
	if err != nil {
		return fmt.Errorf("disconnect broker: %w", err)
	}
	return nil
}

type supervisedRunners struct {
	ctx      context.Context
	group    *errgroup.Group
	count    int
	doneOnce sync.Once
	doneCh   chan error
}

func newSupervisedRunners(ctx context.Context) *supervisedRunners {
	group, groupCtx := errgroup.WithContext(ctx)
	return &supervisedRunners{ctx: groupCtx, group: group, doneCh: make(chan error, 1)}
}

func (r *supervisedRunners) start(name string, run func(context.Context) error) {
	if run == nil {
		return
	}
	r.count++
	r.group.Go(func() error {
		err := run(r.ctx)
		if errors.Is(err, context.Canceled) || r.ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s stopped: %w", name, err)
		}
		return fmt.Errorf("%s stopped unexpectedly", name)
	})
}

func (r *supervisedRunners) done() <-chan error {
	if r.count == 0 {
		return nil
	}
	r.doneOnce.Do(func() {
		go func() {
			r.doneCh <- r.group.Wait()
		}()
	})
	return r.doneCh
}

func (r *supervisedRunners) wait() error {
	if r.count == 0 {
		return nil
	}
	return <-r.done()
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

func webhookBridgePostureProbe(cfg config.WebhookBridgeConfig, ready func() bool) func() *contracts.WebhookBridgePosture {
	if !cfg.Enabled {
		return nil
	}
	return func() *contracts.WebhookBridgePosture {
		sourceCIDRCount := len(cfg.ProviderSourceCIDRs)
		runtimeTargetConfigured := cfg.TargetURL != ""
		bridgeReady := false
		if ready != nil {
			bridgeReady = ready()
		}
		return &contracts.WebhookBridgePosture{
			Enabled:                 true,
			Ready:                   bridgeReady,
			LoopbackOnly:            sourceCIDRCount == 0,
			SourceCIDRsConfigured:   sourceCIDRCount > 0,
			ProviderCIDRCount:       sourceCIDRCount,
			RuntimeTargetConfigured: runtimeTargetConfigured,
			BodyLimitBytes:          cfg.MaxBodyBytes,
			RequestTimeoutMS:        cfg.RequestTimeoutMS,
		}
	}
}

func weeklyReportRequestFromConfig(cfg config.WeeklyReportConfig) reporting.WeeklyReportRequest {
	return reporting.WeeklyReportRequest{
		DeviceID:     cfg.DeviceID,
		CustomerName: cfg.CustomerName,
		SiteName:     cfg.SiteName,
		Timezone:     cfg.Timezone,
		SensorIDs:    append([]string(nil), cfg.SensorIDs...),
	}
}

type gatewayAuthSecrets struct {
	Enabled        bool
	CurrentSecret  string
	PreviousSecret string
}

func heartbeatAuthFromConfig(auth config.GatewayAuthConfig) (heartbeat.AuthConfig, error) {
	secrets, err := gatewayAuthSecretsFromConfig(auth)
	if err != nil {
		return heartbeat.AuthConfig{}, err
	}
	return heartbeatAuthFromSecrets(secrets), nil
}

func heartbeatAuthFromSecrets(secrets gatewayAuthSecrets) heartbeat.AuthConfig {
	if !secrets.Enabled {
		return heartbeat.AuthConfig{}
	}
	return heartbeat.AuthConfig{Enabled: true, SharedSecret: secrets.CurrentSecret}
}

func gatewayAuthSecretsFromConfig(auth config.GatewayAuthConfig) (gatewayAuthSecrets, error) {
	if !auth.Enabled {
		return gatewayAuthSecrets{}, nil
	}
	envName := strings.TrimSpace(auth.SharedSecretEnv)
	if envName == "" {
		envName = config.DefaultGatewayAuthSecretEnv
	}
	secret := strings.TrimSpace(os.Getenv(envName))
	if secret == "" {
		return gatewayAuthSecrets{}, fmt.Errorf("gateway.auth.enabled is true but environment variable %q is empty", envName)
	}
	previousSecret := ""
	previousEnv := strings.TrimSpace(auth.PreviousSharedSecretEnv)
	if previousEnv != "" {
		previousSecret = strings.TrimSpace(os.Getenv(previousEnv))
		if previousSecret == "" {
			return gatewayAuthSecrets{}, fmt.Errorf("gateway.auth.previous_shared_secret_env is set but environment variable %q is empty", previousEnv)
		}
		if previousSecret == secret {
			return gatewayAuthSecrets{}, fmt.Errorf("gateway.auth.previous_shared_secret_env must reference a secret different from gateway.auth.shared_secret_env")
		}
	}
	return gatewayAuthSecrets{Enabled: true, CurrentSecret: secret, PreviousSecret: previousSecret}, nil
}

func runtimeClientOptionsFromSecrets(
	encryption config.GatewayEncryptionConfig,
	secrets gatewayAuthSecrets,
	now func() time.Time,
) ([]runtimeclient.MQTTClientOption, error) {
	if !secrets.Enabled {
		if encryption.Enabled {
			return nil, fmt.Errorf("gateway.encryption.enabled requires gateway.auth.enabled")
		}
		return nil, nil
	}
	options := []runtimeclient.MQTTClientOption{
		runtimeclient.WithMessageAuth(secrets.CurrentSecret, secrets.PreviousSecret, now),
	}
	if encryption.Enabled {
		options = append(options, runtimeclient.WithMessageEncryption(secrets.CurrentSecret))
	}
	return options, nil
}

func runtimeHeartbeatHandlerFromSecrets(
	secrets gatewayAuthSecrets,
	registry *site.Registry,
	now func() time.Time,
) (*site.RuntimeHeartbeatHandler, error) {
	var verifier *mqttauth.Verifier
	if secrets.Enabled {
		var err error
		verifier, err = mqttauth.NewVerifier(mqttauth.Config{
			SharedSecret:         secrets.CurrentSecret,
			PreviousSharedSecret: secrets.PreviousSecret,
			Now:                  now,
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
	if deps.newReportingProvider == nil {
		deps.newReportingProvider = defaults.newReportingProvider
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
	if deps.newRuntimeClient == nil {
		deps.newRuntimeClient = defaults.newRuntimeClient
	}
	if deps.newWeeklyReportRunner == nil {
		deps.newWeeklyReportRunner = defaults.newWeeklyReportRunner
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
