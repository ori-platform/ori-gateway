// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/custody"
	"github.com/ori-platform/ori-gateway/internal/evidence/faults"
	"github.com/ori-platform/ori-gateway/internal/evidence/signingclock"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ori-platform/ori-gateway/internal/asynclog"
	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/dispatcher"
	"github.com/ori-platform/ori-gateway/internal/enrichment"
	"github.com/ori-platform/ori-gateway/internal/evidence/courier"
	"github.com/ori-platform/ori-gateway/internal/fleet"
	"github.com/ori-platform/ori-gateway/internal/gatewaystate"
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
	defaultEvidenceClientID    = "ori-gateway-evidence"
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
	// Publishing reports whether heartbeats are going out, so site health
	// never shows the gateway healthy once its heartbeat has stopped (GW-6).
	Publishing() bool
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
	// authorityTrustOwner is the one user besides root trusted to own the
	// authority CA bundle and the directories above it: zero, root, outside tests.
	authorityTrustOwner int
	// signingMark, when set by a test, wraps the state directory's mark
	// before the signing clock uses it, as a slow disk would.
	signingMark func(mqttauth.DurableMark) mqttauth.DurableMark
	// stateQuiescenceBound is how long stopping waits for an outstanding
	// write of the signing clock's mark before the process ends without
	// releasing the state directory (gateway-config/v2 rule 13).
	stateQuiescenceBound time.Duration
	// exitProcess ends the process; a test records the call instead.
	exitProcess func(code int)
	logger      *slog.Logger
	now         func() time.Time
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
		stateQuiescenceBound: defaultStateQuiescenceBound,
		exitProcess:          os.Exit,
		logger:               slog.Default(),
		now:                  time.Now,
	}
}

// defaultStateQuiescenceBound is this release's quiescence bound under
// gateway-config/v2 gateway.state_directory rule 13.
const defaultStateQuiescenceBound = 5 * time.Second

// errStateWriteOutstanding is why a stopping gateway ends its process: a
// write of the signing clock's mark is still outstanding.
var errStateWriteOutstanding = errors.New("a write of the signing clock mark is still outstanding")

// abandonedOwners holds every state directory hold a run could not release,
// so nothing closes its lock before the process ends.
var (
	abandonedMu     sync.Mutex
	abandonedOwners []*gatewaystate.Owner
)

// awaitQuiescence waits for idle, the end of the outstanding write of the
// mark, until the quiescence bound has passed since started, and reports
// whether it ended (gateway-config/v2 gateway.state_directory rule 13).
// Shortly before the bound it warns, off this path, that the process will end
// unless the write completes, so the line has its chance to be written
// without ever delaying the exit.
func awaitQuiescence(deps appDependencies, idle <-chan struct{}, started time.Time) bool {
	bound := deps.stateQuiescenceBound
	deadline := time.NewTimer(time.Until(started.Add(bound)))
	defer deadline.Stop()
	warn := time.NewTimer(time.Until(started.Add(bound - quiescenceWarningLead(bound))))
	defer warn.Stop()
	for {
		select {
		case <-idle:
			return true
		case <-warn.C:
			go deps.logger.Warn("a write of the signing clock mark is still outstanding; the process ends at the quiescence bound unless it completes",
				"bound", bound)
		case <-deadline.C:
			select {
			case <-idle:
				return true
			default:
				return false
			}
		}
	}
}

// quiescenceWarningLead is how long before the quiescence bound the gateway
// warns that it will end the process.
func quiescenceWarningLead(bound time.Duration) time.Duration {
	return min(100*time.Millisecond, bound/10)
}

// abandonStateDirectory ends the process with a non-zero status while a write
// of the mark is still outstanding, without releasing the state directory:
// the lock ends with the process, so no second owner can take it while that
// write may still land (gateway-config/v2 gateway.state_directory rules 3
// and 13). No further run begins in this process. Nothing on this path waits
// on the logger: its error line is written off it, and may be lost.
func abandonStateDirectory(deps appDependencies, owner *gatewaystate.Owner, cause error) error {
	abandonedMu.Lock()
	abandonedOwners = append(abandonedOwners, owner)
	abandonedMu.Unlock()
	go deps.logger.Error("gateway state directory is not quiescent; ending the process without releasing it",
		"error", cause, "bound", deps.stateQuiescenceBound)
	deps.exitProcess(1)
	return fmt.Errorf("gateway state directory not quiescent: %w", cause)
}

func runCLI(args []string, stdout io.Writer, stderr io.Writer) int {
	return runCLIWith(args, stdout, stderr, defaultDependencies)
}

// runCLIWith is runCLI with the dependencies the run is given, so a test can
// drive the whole process path, its exit status included.
func runCLIWith(args []string, stdout io.Writer, stderr io.Writer, dependencies func() appDependencies) int {
	flags := flag.NewFlagSet("ori-gateway", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", defaultConfigPath, "path to gateway.yaml")
	showVersion := flags.Bool("version", false, "print version and build provenance, then exit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		writeVersion(stdout, gatewayProvenance(debug.ReadBuildInfo()))
		return 0
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	deps := dependencies()
	if err := runGateway(ctx, *configPath, deps); err != nil {
		// The process exits whether or not stderr takes the line: a log
		// destination that blocks never keeps an escalation from ending it
		// (GW-6).
		writeWithin(stderr, fmt.Sprintf("ori-gateway: %v\n", err), asynclog.ExitFlushBound)
		return 1
	}
	writeWithin(stdout, "ori-gateway stopped\n", asynclog.ExitFlushBound)
	return 0
}

// writeWithin writes line to w, waiting at most bound for it; a writer that
// blocks is left to finish, or not, on its own.
func writeWithin(w io.Writer, line string, bound time.Duration) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.WriteString(w, line)
	}()
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

func runGateway(ctx context.Context, configPath string, deps appDependencies) (runErr error) {
	deps = normalizeDependencies(deps)
	// Every record the gateway logs is written off its caller's path, so a log
	// destination that stops accepting writes never stalls startup, a
	// callback or an escalation on its way to ending the process (GW-6).
	// What is queued when the run ends gets a bounded chance to be written.
	queuedLog, logs := asynclog.New(deps.logger.Handler(), "gateway log lines dropped behind a logger that blocked")
	deps.logger = queuedLog
	defer logs.Flush(asynclog.ExitFlushBound)
	startedAt := deps.now()

	cfg, err := deps.loadConfig(configPath)
	if err != nil {
		return err
	}
	gatewaySecrets, err := gatewayAuthSecretsFromConfig(cfg.Gateway.Auth)
	if err != nil {
		return err
	}
	custodySecret, err := resolveCustodySecret(cfg.Gateway.Custody, gatewaySecrets)
	if err != nil {
		return err
	}
	// The signer is built during startup so custody key material is validated
	// where a failure stops the gateway, rather than at the first envelope the
	// courier takes custody of. A gateway that cannot derive its own key_id has
	// no custody to offer, and reporting healthy while the runtime waits for
	// acknowledgements that can never arrive is the failure mode this ordering
	// removes. It is held rather than used: nothing routes an acknowledgement
	// outbound until the inbound path lands.
	var custodySigner *custody.Signer
	if custodySecret != "" {
		custodySigner, err = custody.NewSigner(custodySecret)
		if err != nil {
			return fmt.Errorf("construct custody signer: %w", err)
		}
	}

	// One signing clock for every envelope the gateway signs outside the
	// evidence path, so no two are signed at one value (gateway-api/v2
	// Envelope Freshness and Replay, Senders). A gateway that signs keeps its
	// mark in its own state directory, independent of evidence forwarding
	// (gateway-config/v2 gateway.state_directory). A directory that is not
	// private, not the gateway's, corrupt or not writable refuses the start,
	// before any optional module writes to disk. The run holds the state
	// directory exclusively: a second gateway on it is refused, and the hold
	// is released last, once every signer has stopped and no write of the
	// mark is outstanding. When that cannot be reached within the quiescence
	// bound, the process ends without releasing it (rule 13).
	signingClock := mqttauth.NewSigningClock(deps.now, deps.logger)
	stopSigning := func() {}
	if cfg.Gateway.Auth.Enabled {
		owner, err := gatewaystate.Acquire(cfg.Gateway.StateDirectory)
		if err != nil {
			return fmt.Errorf("open gateway state directory: %w", err)
		}
		var mark mqttauth.DurableMark = owner.Mark()
		if deps.signingMark != nil {
			mark = deps.signingMark(mark)
		}
		signingClock, err = mqttauth.NewDurableSigningClock(deps.now, mark, deps.logger)
		var stalled *mqttauth.StartWriteError
		switch {
		case errors.As(err, &stalled):
			// The bound runs from the decision to refuse.
			if !awaitQuiescence(deps, stalled.Done(), time.Now()) {
				return abandonStateDirectory(deps, owner, err)
			}
			_ = owner.Close()
			return fmt.Errorf("open gateway signing clock: %w", err)
		case err != nil:
			_ = owner.Close()
			return fmt.Errorf("open gateway signing clock: %w", err)
		}
		// Deciding to stop stops the clock at once, and the quiescence bound
		// runs from that decision, once for the whole stop: past it with a
		// write outstanding, the process ends even while callbacks are still
		// being joined.
		quiescent := make(chan error, 1)
		var stopOnce sync.Once
		stopSigning = func() {
			stopOnce.Do(func() {
				decided := time.Now()
				idle := signingClock.Stop()
				go func() {
					if awaitQuiescence(deps, idle, decided) {
						quiescent <- nil
						return
					}
					quiescent <- abandonStateDirectory(deps, owner, errStateWriteOutstanding)
				}()
			})
		}
		// Registered first, so it runs last: after every signer has stopped.
		defer func() {
			stopSigning()
			if err := <-quiescent; err != nil {
				runErr = errors.Join(runErr, err)
				return
			}
			_ = owner.Close()
		}()
	}

	var evidenceQueue *courier.DurableQueue
	var evidenceFaults *faults.Recorder
	var evidenceWorker *courier.DeliveryWorker
	var authoritySink *courier.DurableAuthoritySink
	if cfg.Evidence.Enabled {
		if !gatewaySecrets.Enabled || custodySigner == nil {
			return fmt.Errorf("evidence courier requires gateway auth and dedicated custody configuration")
		}
		channelConfig, err := resolveEvidenceChannel(cfg.Evidence, gatewaySecrets, custodySecret)
		if err != nil {
			return err
		}
		// Once any device declares gateway-evidence-carriage/v1, capacity is
		// reserved per configured device: each holds an equal share of
		// max_items and max_bytes, so one device cannot exhaust another's. A
		// site that declares nothing keeps one gateway-wide share.
		evidenceFaults = faults.NewRecorder()
		evidenceQueue, err = courier.OpenDurableQueue(courier.QueueOptions{
			Directory: cfg.Evidence.QueueDirectory, MaxItems: cfg.Evidence.MaxItems, MaxBytes: cfg.Evidence.MaxBytes,
			Devices: cfg.Gateway.DeviceIDs, ReserveDeviceShares: cfg.Evidence.DeclaresVersionedCarriage(),
			VersionedDevices: cfg.Evidence.VersionedDevices(),
			Faults:           evidenceFaults, FaultSource: "outbound", Now: deps.now,
		})
		if err != nil {
			return fmt.Errorf("open evidence outbound queue: %w", err)
		}
		authoritySink, err = courier.OpenDurableAuthoritySink(courier.QueueOptions{
			Directory: cfg.Evidence.ReturnQueueDirectory, MaxItems: cfg.Evidence.MaxItems, MaxBytes: cfg.Evidence.MaxBytes,
			Faults: evidenceFaults, FaultSource: "return", Now: deps.now,
		})
		if err != nil {
			return fmt.Errorf("open evidence return queue: %w", err)
		}
		var authorityCAs *x509.CertPool
		if cfg.Evidence.AuthorityCAFile != "" {
			authorityCAs, err = courier.LoadAuthorityTrust(cfg.Evidence.AuthorityCAFile, deps.authorityTrustOwner)
			if err != nil {
				return err
			}
		}
		channel, err := courier.NewHTTPChannel(courier.HTTPChannelOptions{
			Endpoint: channelConfig.endpoint, ClientID: channelConfig.clientID, Secret: channelConfig.secret, Now: deps.now,
			AuthorityCAs: authorityCAs,
		})
		if err != nil {
			return fmt.Errorf("construct independent evidence channel")
		}
		backoffBase, backoffBound := cfg.Evidence.Backoff()
		evidenceWorker, err = courier.NewDeliveryWorker(
			evidenceQueue, channel, authoritySink,
			courier.DeliveryWorkerOptions{
				RetryInterval: time.Duration(cfg.Evidence.RetryIntervalS) * time.Second,
				BackoffBase:   backoffBase,
				MaxBackoff:    backoffBound,
				Faults:        evidenceFaults,
				Logger:        deps.logger,
			},
		)
		if err != nil {
			return fmt.Errorf("construct evidence delivery worker: %w", err)
		}
	}

	reasoningProvider, err := deps.newProvider(cfg.Provider)
	if err != nil {
		return fmt.Errorf("construct provider: %w", err)
	}
	providerStatus := providerStatusFromProvider(reasoningProvider)

	var webhookBridge webhookBridgeRunner
	var webhookBridgeReady atomic.Bool
	webhookBridgeRefusals := webhookbridge.NewRefusals()
	// Every webhook request is admitted through its own gate, which the
	// decision to stop refuses with the broker callbacks (rule 13).
	webhookIngress := newCallbackGate()
	if cfg.WebhookBridge.Enabled {
		webhookBridge, err = deps.newWebhookBridge(cfg.WebhookBridge, webhookbridge.Options{
			Logger:   deps.logger,
			Now:      deps.now,
			Refusals: webhookBridgeRefusals,
			Admit:    webhookIngress.admit,
		})
		if err != nil {
			return fmt.Errorf("construct webhook bridge: %w", err)
		}
	}

	// Every broker callback runs through one gate. The decision to stop
	// refuses any later callback at once; shutdown then waits for those
	// running, before any state they may sign or write is released (GW-15;
	// gateway-config/v2 gateway.state_directory rule 13).
	callbacks := newCallbackGate()

	// One count of size and volume refusals for every broker client, projected
	// as the site's mqtt_ingress posture.
	ingress := broker.NewIngressStats(deps.logger)
	rawClient, err := deps.newBroker(broker.Options{
		BrokerURL: cfg.Gateway.BrokerURL,
		ClientID:  defaultClientID,
		Logger:    deps.logger,
		Ingress:   ingress,
		Devices:   cfg.Gateway.DeviceIDs,
	})
	if err != nil {
		return fmt.Errorf("construct broker: %w", err)
	}
	client := callbacks.wrap(rawClient)
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect broker: %w", err)
	}
	disconnect := true
	defer func() {
		if disconnect {
			_ = client.Disconnect(context.Background())
		}
	}()
	var evidenceBroker brokerClient
	if cfg.Evidence.Enabled {
		rawEvidenceBroker, err := deps.newBroker(broker.Options{
			BrokerURL: cfg.Gateway.BrokerURL, ClientID: defaultEvidenceClientID,
			Logger: deps.logger, PersistentSession: true, Ingress: ingress,
			Devices: cfg.Gateway.DeviceIDs,
		})
		if err != nil {
			return fmt.Errorf("construct evidence broker: %w", err)
		}
		evidenceBroker = callbacks.wrap(rawEvidenceBroker)
		if err := evidenceBroker.Connect(ctx); err != nil {
			return fmt.Errorf("connect evidence broker: %w", err)
		}
		defer func() { _ = evidenceBroker.Disconnect(context.Background()) }()
	}

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

	var runtimeExportClient runtimeclient.Client
	if cfg.Reporting.WeeklyReport.Enabled || cfg.SiteHealth.Enabled {
		runtimeClientOptions, err := runtimeClientOptionsFromSecrets(cfg.Gateway.Encryption, gatewaySecrets, deps.now)
		if err != nil {
			return fmt.Errorf("construct runtime export client options: %w", err)
		}
		// The client's own background work ends with this run.
		clientLifetime, endClient := context.WithCancel(ctx)
		defer endClient()
		runtimeClientOptions = append(runtimeClientOptions, runtimeclient.WithLogger(deps.logger),
			runtimeclient.WithSigningClock(signingClock), runtimeclient.WithLifetime(clientLifetime))
		runtimeExportClient, err = deps.newRuntimeClient(client, runtimeClientOptions...)
		if err != nil {
			return fmt.Errorf("construct runtime export client: %w", err)
		}
	}

	var weeklyReport weeklyReportRunner
	if cfg.Reporting.WeeklyReport.Enabled {
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
			cloudCfg := cfg.Reporting.WeeklyReport.Delivery.Cloud
			apiKey := strings.TrimSpace(os.Getenv(cloudCfg.AuthEnv))
			if apiKey == "" {
				return fmt.Errorf("weekly report cloud deliverer: env var %q is empty or not set", cloudCfg.AuthEnv)
			}
			cd, err := reporting.NewCloudDeliverer(cloudCfg.Endpoint, apiKey, reporting.CloudDelivererOptions{})
			if err != nil {
				return fmt.Errorf("weekly report cloud deliverer: %w", err)
			}
			deliverers = append(deliverers, cd)
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
	// Posture fetches run per device off the broker callback, and each signs
	// an export request from the durable signing clock. They are owned here:
	// once shutdown begins none starts, and runGateway never returns while
	// one is still running (GW-15).
	postureDebounce := newPostureDebouncer()
	decision := &stopDecision{callbacks: callbacks, webhook: webhookIngress, posture: postureDebounce, stopSigning: stopSigning, cancel: cancel}
	decideStop := decision.decide
	defer func() {
		decideStop()
		cancel()
		callbacks.close()
		webhookIngress.close()
		postureDebounce.close()
	}()
	runners := newSupervisedRunners(runCtx)
	// The stop is decided where it happens: when the runner group's context
	// ends, on the parent's cancellation or a runner's failure, before the
	// group cancels and joins its runners, not once that is noticed here.
	stopOnRunnersEnd := context.AfterFunc(runners.ctx, decideStop)
	defer stopOnRunnersEnd()
	shutdownRunners := func() {
		decideStop()
		cancel()
		postureDebounce.close()
		_ = runners.wait()
	}

	if weeklyReport != nil {
		runners.start("weekly report runner", weeklyReport.Run)
	}
	var evidenceIngress *courier.RuntimeIngress
	var returnPublisher *courier.ReturnPublisher
	if evidenceWorker != nil {
		evidenceCourier, err := courier.NewCourier(evidenceQueue, custodySigner)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("construct evidence courier: %w", err)
		}
		ackClock, err := signingclock.OpenAck(cfg.Evidence.QueueDirectory, deps.now)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("open evidence acknowledgement clock: %w", err)
		}
		evidenceIngress, err = courier.NewRuntimeIngress(
			evidenceCourier,
			authoritySink,
			ackClock,
			evidenceBroker.Publish,
			gatewaySecrets.CurrentSecret,
			deps.now,
		)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("construct evidence runtime ingress: %w", err)
		}
		returnClock, err := signingclock.OpenReturn(cfg.Evidence.ReturnQueueDirectory, deps.now)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("open evidence return signing clock: %w", err)
		}
		returnPublisher, err = courier.NewReturnPublisher(
			authoritySink, returnClock, evidenceBroker.Publish, gatewaySecrets.CurrentSecret, gatewaySecrets.PreviousSecret,
			deps.now, time.Duration(cfg.Evidence.RetryIntervalS)*time.Second,
		)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("construct evidence return publisher: %w", err)
		}
		returnPublisher.SetLogger(deps.logger)
		evidenceIngress.SetReturnNotifier(returnPublisher.Notify)
		evidenceIngress.SetHandoff(courier.Handoff(evidenceWorker, returnPublisher))
		evidenceIngress.SetFaultRecorder(evidenceFaults)
		runners.start("evidence delivery worker", evidenceWorker.Run)
		// Each durable store was probed when it opened; these keep probing it,
		// independently of admissions and of health publication.
		probeInterval := cfg.Evidence.StoreProbeInterval()
		runners.start("evidence outbound store probe", func(ctx context.Context) error {
			return evidenceQueue.ProbeLoop(ctx, probeInterval)
		})
		runners.start("evidence return store probe", func(ctx context.Context) error {
			return authoritySink.ProbeLoop(ctx, probeInterval)
		})
		runners.start("evidence return publisher", returnPublisher.Run)
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
	runtimeHeartbeatHandler, err := runtimeHeartbeatHandlerFromSecrets(gatewaySecrets, siteRegistry, deps.now, deps.logger)
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
			SigningClock:  signingClock,
		},
	)
	if err != nil {
		shutdownRunners()
		return fmt.Errorf("construct heartbeat: %w", err)
	}
	runners.start("heartbeat", hb.Run)

	// Supervised like every long-running task, so it stops with the others
	// and never outlives runGateway (GW-15).
	runners.start("site node eviction", func(ctx context.Context) error {
		evictStaleRuntimeNodes(ctx, siteRegistry, site.NodeTTL, deps.logger)
		<-ctx.Done()
		return ctx.Err()
	})

	dispatcherOptions := dispatcher.Options{ProviderTimeoutMS: cfg.Provider.TimeoutMS, Now: deps.now, SigningClock: signingClock}
	if gatewaySecrets.Enabled {
		verifier, err := mqttauth.NewVerifier(mqttauth.Config{
			SharedSecret:         gatewaySecrets.CurrentSecret,
			PreviousSharedSecret: gatewaySecrets.PreviousSecret,
			Now:                  deps.now,
			Logger:               deps.logger,
		})
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("construct reasoning verifier: %w", err)
		}
		dispatcherOptions.AuthVerifier = verifier
		dispatcherOptions.SigningSecret = gatewaySecrets.CurrentSecret
	} else {
		deps.logger.Warn("reasoning is served without gateway.auth; requests are unverified and responses unsigned (development only)")
	}
	reasoningDispatcher, err := dispatcher.New(client, reasoningProvider, dispatcherOptions)
	if err != nil {
		shutdownRunners()
		return fmt.Errorf("construct dispatcher: %w", err)
	}

	requestErr := make(chan error, 1)
	// stopCause is the request failure escalation when a callback decided
	// the stop on it, which ended the run's context: a later step that fails
	// for that reason reports the escalation, not its own cancellation.
	stopCause := func(err error) error {
		select {
		case escalation := <-requestErr:
			return escalation
		default:
			return err
		}
	}
	var requestFailureMu sync.Mutex
	requestFailures := 0
	reasoningRequests := newRequestPool(cfg.Gateway.DeviceIDs, callbacks, ingress, deps.logger, contracts.FamilyReasoningRequest)
	if err := reasoningRequests.subscribe(runCtx, client, contracts.GatewayReasoningRequestTopicFilter, broker.QoSReasoning, func(topic string, payload []byte) {
		if err := reasoningDispatcher.HandleRequest(runCtx, topic, payload); err != nil {
			deps.logger.Warn("reasoning request handling failed", "topic", topic, "error", err)
			// Only a reply that could not be delivered counts toward the
			// escalation (GW-6). A refused request, malformed, unauthenticated,
			// not fresh or with a request_id already in flight, neither counts
			// nor resets the count, so no sender can end the gateway or mask a
			// delivery failure by sending junk or repeating a request.
			if !errors.Is(err, dispatcher.ErrPublishResponse) {
				return
			}
			// A reply the run's own stop refused, by cancelling it or by
			// closing the signing clock, is not a delivery failure.
			if runCtx.Err() != nil || errors.Is(err, mqttauth.ErrSigningClockClosed) {
				return
			}
			requestFailureMu.Lock()
			requestFailures++
			failures := requestFailures
			requestFailureMu.Unlock()
			if failures >= defaultRequestFailureLimit {
				// The escalation is recorded first, so whatever the decision's
				// cancellation ends reports it. The stop is then decided here,
				// where it happens, not once the escalation is noticed;
				// refusing never waits, so it is safe from inside an admitted
				// callback.
				select {
				case requestErr <- fmt.Errorf("reasoning request failure limit reached: %w", err):
				default:
				}
				decideStop()
			}
			return
		}
		requestFailureMu.Lock()
		requestFailures = 0
		requestFailureMu.Unlock()
	}); err != nil {
		shutdownRunners()
		return stopCause(fmt.Errorf("subscribe reasoning requests: %w", err))
	}

	if evidenceIngress != nil {
		allowedDevices := make(map[string]bool, len(cfg.Gateway.DeviceIDs))
		for _, deviceID := range cfg.Gateway.DeviceIDs {
			allowedDevices[deviceID] = true
		}
		// Each carriage's acknowledgement waits for its PUBACK, so carriages
		// are handled off the evidence client's delivery. Carriages and inbound
		// acknowledgements share one queue per device, so each device's are
		// handled in the order they arrived, as on the client.
		evidenceTraffic := newEvidencePool(cfg.Gateway.DeviceIDs, callbacks, ingress, deps.logger)
		if err := evidenceTraffic.subscribe(runCtx, evidenceBroker, contracts.EvidenceOutboundTopicFilter, broker.QoSReasoning, func(topic string, payload []byte) {
			deviceID, parseErr := contracts.DeviceIDFromEvidenceOutboundTopic(topic)
			if parseErr != nil {
				deps.logger.Warn("evidence outbound transport refused")
				return
			}
			// An unconfigured device is rejected before admission: nothing is
			// admitted, no custody is issued and no acknowledgement is
			// published, so the runtime keeps its bytes and retries
			// (gateway-api/v1). The observation raises device_unconfigured,
			// which never carries the device ID.
			if !allowedDevices[deviceID] {
				_ = evidenceQueue.NoteUnconfiguredDevice(deviceID)
				deps.logger.Warn("evidence outbound carriage for an unconfigured device refused")
				return
			}
			// An admission is a handoff, through the ingress's handoff hook; a
			// carriage it refuses, with or without an acknowledgement, is not.
			if err := evidenceIngress.Handle(runCtx, topic, payload); err != nil {
				deps.logger.Warn("evidence outbound transport refused")
			}
		}); err != nil {
			shutdownRunners()
			return stopCause(fmt.Errorf("subscribe outbound evidence: %w", err))
		}
		for _, deviceID := range cfg.Gateway.DeviceIDs {
			ackTopic, err := contracts.EvidenceInboundAckTopic(deviceID)
			if err != nil {
				shutdownRunners()
				return fmt.Errorf("evidence inbound acknowledgement topic: %w", err)
			}
			if err := evidenceTraffic.subscribe(runCtx, evidenceBroker, ackTopic, broker.QoSReasoning, func(topic string, payload []byte) {
				logInboundAckResult(deps.logger, returnPublisher.HandleAck(topic, payload))
			}); err != nil {
				shutdownRunners()
				return stopCause(fmt.Errorf("subscribe evidence inbound acknowledgements: %w", err))
			}
		}
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
				return
			}
			if runtimeExportClient != nil {
				if id, err := contracts.DeviceIDFromRuntimeNodeHeartbeatTopic(topic); err == nil {
					if postureDebounce.tryStart(id) {
						go func() {
							defer postureDebounce.done(id)
							fetchAndUpsertPosture(runCtx, runtimeExportClient, siteRegistry, id, deps.logger)
						}()
					}
				}
			}
		}); err != nil {
			shutdownRunners()
			return stopCause(fmt.Errorf("subscribe runtime node heartbeats: %w", err))
		}
	}

	if cfg.Reporting.TierCEnrichment.Enabled {
		enrichmentProvider, err := deps.newTierCEnrichmentProvider(cfg.Reporting)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("construct tier c enrichment provider: %w", err)
		}
		enrichmentOptions := enrichment.Options{Logger: deps.logger, Now: deps.now, SigningClock: signingClock}
		if gatewaySecrets.Enabled {
			verifier, err := mqttauth.NewVerifier(mqttauth.Config{
				SharedSecret:         gatewaySecrets.CurrentSecret,
				PreviousSharedSecret: gatewaySecrets.PreviousSecret,
				Now:                  deps.now,
				Logger:               deps.logger,
			})
			if err != nil {
				shutdownRunners()
				return fmt.Errorf("construct tier c enrichment verifier: %w", err)
			}
			enrichmentOptions.AuthVerifier = verifier
			enrichmentOptions.SigningSecret = gatewaySecrets.CurrentSecret
		} else {
			deps.logger.Warn("tier c enrichment is enabled without gateway.auth; requests are unverified and responses unsigned (development only)")
		}
		enrichmentHandler, err := enrichment.NewHandler(client, enrichmentProvider, enrichmentOptions)
		if err != nil {
			shutdownRunners()
			return fmt.Errorf("construct tier c enrichment handler: %w", err)
		}
		enrichmentRequests := newRequestPool(cfg.Gateway.DeviceIDs, callbacks, ingress, deps.logger, contracts.FamilyEnrichmentRequest)
		for _, deviceID := range cfg.Gateway.DeviceIDs {
			topic, err := contracts.TierCEnrichmentRequestTopic(deviceID)
			if err != nil {
				shutdownRunners()
				return fmt.Errorf("tier c enrichment request topic: %w", err)
			}
			if err := enrichmentRequests.subscribe(runCtx, client, topic, broker.QoSReasoning, func(topic string, payload []byte) {
				if err := enrichmentHandler.HandleRequest(runCtx, topic, payload); err != nil {
					deps.logger.Warn("tier c enrichment request handling failed", "topic", topic, "error", err)
				}
			}); err != nil {
				shutdownRunners()
				return stopCause(fmt.Errorf("subscribe tier c enrichment requests: %w", err))
			}
		}
	}

	if cfg.SiteHealth.Enabled {
		siteProjector := site.NewProjector(siteRegistry, site.ProjectOptions{
			ExpectedDeviceIDs: cfg.Gateway.DeviceIDs,
			NodeTTL:           site.NodeTTL,
		})
		gatewayViewFn := func() site.GatewayView {
			status := site.SiteStatusHealthy
			evidenceDelivery := evidenceDeliveryView(evidenceWorker)
			if !providerStatus.Healthy(runCtx) || signingClock.Degraded() || !hb.Publishing() ||
				(evidenceDelivery != nil && (evidenceDelivery.Degraded || evidenceDelivery.Blocked)) {
				status = site.SiteStatusDegraded
			}
			return site.GatewayView{
				Status:               status,
				ProviderName:         providerStatus.Name(),
				UptimeS:              deps.now().Sub(startedAt).Seconds(),
				WebhookBridgeEnabled: cfg.WebhookBridge.Enabled,
				WebhookBridgeReady:   webhookBridgeReady.Load(),
				WebhookBridgeRefused: site.WebhookBridgeRefused{
					Sender: webhookBridgeRefusals.Sender(), Global: webhookBridgeRefusals.Global(),
				},
				EvidenceDelivery: evidenceDelivery,
				MQTTIngress:      mqttIngressView(ingress),
			}
		}
		runners.start("site health server", site.NewHealthHandler(
			siteProjector, gatewayViewFn, cfg.SiteHealth.ListenAddr, deps.now,
		).Run)
	}

	select {
	case <-ctx.Done():
		shutdownRunners()
		// A stop a callback already decided on the escalation is reported as
		// that, even when the caller cancels before it is noticed.
		return stopCause(disconnectBroker(client, &disconnect))
	case err := <-runners.done():
		decideStop()
		cancel()
		postureDebounce.close()
		if escalation := stopCause(nil); escalation != nil {
			return escalation
		}
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

func evidenceDeliveryView(worker *courier.DeliveryWorker) *site.GatewayEvidenceDeliveryView {
	if worker == nil {
		return nil
	}
	status := worker.Status()
	view := &site.GatewayEvidenceDeliveryView{
		Pending:   status.Pending,
		Degraded:  status.Degraded,
		Blocked:   status.Blocked,
		LastError: status.LastError,
	}
	if !status.LastFailureAt.IsZero() {
		view.LastFailureAtMS = status.LastFailureAt.UnixMilli()
	}
	view.Faults = append([]string{}, status.Faults...)
	view.Devices = make([]site.GatewayEvidenceDeliveryDevice, 0, len(status.Devices))
	for _, device := range status.Devices {
		entry := site.GatewayEvidenceDeliveryDevice{
			DeviceID: device.DeviceID, Lane: string(device.Lane), Pending: device.Pending, State: device.State,
		}
		if device.Held != nil {
			entry.Held = &site.GatewayEvidenceDeliveryHeld{
				QueueRecord: device.Held.QueueRecord, ArtifactDigest: device.Held.ArtifactDigest,
				ArtifactType: string(device.Held.ArtifactType), RefusalStatus: device.Held.RefusalStatus,
				Reason: device.Held.Reason, FirstHeldAtMS: device.Held.FirstHeldAt.UnixMilli(),
				Acknowledged: device.Held.Acknowledged,
			}
		}
		view.Devices = append(view.Devices, entry)
	}
	return view
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
	ttl time.Duration,
	logger *slog.Logger,
) {
	if registry == nil {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	// Receipt age is measured on the registry's own monotonic clock. A node is
	// evicted at most a third of the TTL after it falls silent past it.
	ticker := time.NewTicker(ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			evicted, retained := registry.EvictStaleRetainingCritical(ttl)
			for _, node := range evicted {
				logger.Warn("runtime node heartbeat stale", "device_id", node.DeviceID, "last_seen_ms", node.LastSeenMS)
			}
			for _, node := range retained {
				if node.Status != site.NodeStatusCritical {
					logger.Error("runtime node silent after a critical health export; kept critical until its export reads otherwise",
						"device_id", node.DeviceID, "gateway_seen_ms", node.GatewaySeen, "last_seen_ms", node.LastSeenMS)
					continue
				}
				logger.Error("runtime node silent after a critical heartbeat; kept critical until it reports again",
					"device_id", node.DeviceID, "gateway_seen_ms", node.GatewaySeen, "last_seen_ms", node.LastSeenMS)
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

// resolveCustodySecret returns the secret this gateway signs custody
// acknowledgements with, or "" when custody is not configured.
//
// Separation from the envelope secret is enforced here on the resolved bytes.
// Configuration compares the variable names, which is necessary and not
// sufficient: two names can hold one value, and a name check would call that
// configuration correct while the courier signed custody with the same key it
// uses for ordinary traffic.
func resolveCustodySecret(
	custody config.GatewayCustodyConfig,
	envelope gatewayAuthSecrets,
) (string, error) {
	envName := strings.TrimSpace(custody.SecretEnv)
	if envName == "" {
		// Not configured is a choice: the courier simply issues no custody.
		return "", nil
	}
	// Read exactly as provisioned. The custody MAC keys from these bytes, so
	// trimming would key from bytes the operator did not set and derive an
	// identifier no conforming peer reproduces; custody.NewSigner
	// refuses surrounding whitespace instead.
	secret := os.Getenv(envName)
	if secret == "" {
		// Configured and empty is a broken credential, not a decision. Reporting
		// it as "custody not in use" would leave a gateway looking healthy while
		// the runtime waited for acknowledgements that never come.
		return "", fmt.Errorf("gateway.custody.secret_env names %q but that environment variable is empty", envName)
	}
	if secret == envelope.CurrentSecret || (envelope.PreviousSecret != "" && secret == envelope.PreviousSecret) {
		return "", fmt.Errorf("gateway.custody.secret_env resolves to a runtime-gateway envelope secret; custody requires key material of its own")
	}
	return secret, nil
}

type resolvedEvidenceChannel struct {
	endpoint string
	clientID string
	secret   string
}

func resolveEvidenceChannel(
	cfg config.EvidenceConfig,
	envelope gatewayAuthSecrets,
	custodySecret string,
) (resolvedEvidenceChannel, error) {
	values := map[string]string{
		"endpoint":  strings.TrimSpace(os.Getenv(cfg.EndpointEnv)),
		"client_id": strings.TrimSpace(os.Getenv(cfg.ClientIDEnv)),
		"secret":    os.Getenv(cfg.SecretEnv),
	}
	for field, value := range values {
		if value == "" {
			return resolvedEvidenceChannel{}, fmt.Errorf("evidence %s environment variable is empty", field)
		}
	}
	secret := values["secret"]
	if strings.TrimSpace(secret) != secret {
		return resolvedEvidenceChannel{}, fmt.Errorf("evidence ingest secret carries surrounding whitespace")
	}
	if secret == custodySecret || secret == envelope.CurrentSecret || (envelope.PreviousSecret != "" && secret == envelope.PreviousSecret) {
		return resolvedEvidenceChannel{}, fmt.Errorf("evidence ingest secret must be distinct from custody and MQTT envelope secrets")
	}
	return resolvedEvidenceChannel{endpoint: values["endpoint"], clientID: values["client_id"], secret: secret}, nil
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
	logger *slog.Logger,
) (*site.RuntimeHeartbeatHandler, error) {
	var verifier *mqttauth.Verifier
	if secrets.Enabled {
		var err error
		verifier, err = mqttauth.NewVerifier(mqttauth.Config{
			SharedSecret:         secrets.CurrentSecret,
			PreviousSharedSecret: secrets.PreviousSecret,
			Now:                  now,
			Logger:               logger,
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
	if deps.stateQuiescenceBound <= 0 {
		deps.stateQuiescenceBound = defaults.stateQuiescenceBound
	}
	if deps.exitProcess == nil {
		deps.exitProcess = defaults.exitProcess
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

// postureDebouncer ensures at most one posture-fetch goroutine runs per device
// at any time. Heartbeats that arrive while a fetch is already in flight are
// skipped; the next heartbeat after the fetch completes will trigger a new one.
type postureDebouncer struct {
	mu       sync.Mutex
	inflight map[string]bool
	closed   bool
	running  sync.WaitGroup
}

func newPostureDebouncer() *postureDebouncer {
	return &postureDebouncer{inflight: make(map[string]bool)}
}

// tryStart marks deviceID as in-flight and returns true if no fetch was already
// running and the debouncer is not closed. Returns false (and does nothing)
// otherwise. A true result must be paired with done.
func (d *postureDebouncer) tryStart(deviceID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.inflight[deviceID] {
		return false
	}
	d.inflight[deviceID] = true
	d.running.Add(1)
	return true
}

func (d *postureDebouncer) done(deviceID string) {
	d.mu.Lock()
	delete(d.inflight, deviceID)
	d.mu.Unlock()
	d.running.Done()
}

// refuse refuses every later fetch, without waiting for those running.
func (d *postureDebouncer) refuse() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
}

// close refuses every later fetch and waits for those running to finish. It
// may be called more than once.
func (d *postureDebouncer) close() {
	d.refuse()
	d.running.Wait()
}

func fetchAndUpsertPosture(ctx context.Context, rc runtimeclient.Client, registry *site.Registry, deviceID string, logger *slog.Logger) {
	snapshot, err := rc.Health(ctx, runtimeclient.HealthRequest{DeviceID: deviceID})
	if err != nil {
		logger.Warn("runtime health fetch failed for posture update", "device_id", deviceID, "error", err)
		return
	}
	registry.UpsertPosture(deviceID, sitePostureFromHealth(snapshot))
}

func sitePostureFromHealth(h runtimeclient.HealthSnapshot) site.SiteNodePosture {
	posture := site.SiteNodePosture{ExportSeverity: siteExportSeverity(h.Status)}
	if h.GatewayBrokerPosture != nil {
		b := h.GatewayBrokerPosture
		posture.BrokerHardening = &site.SiteNodeBrokerPosture{
			Available:             b.Available,
			GatewayEnabled:        b.GatewayEnabled,
			DeploymentCheck:       b.DeploymentCheck,
			AnonymousAccess:       b.AnonymousAccess,
			ACLPolicy:             b.ACLPolicy,
			RequireCredentials:    b.RequireCredentials,
			CredentialsConfigured: b.CredentialsConfigured,
			RequiresACLHardening:  b.RequiresACLHardening,
		}
	}
	if h.StateStoreEncryption != nil {
		e := h.StateStoreEncryption
		posture.Encryption = &site.SiteNodeEncryptionPosture{
			Available:            e.Available,
			Mode:                 e.Mode,
			Satisfied:            e.Satisfied,
			MarkerConfigured:     e.MarkerConfigured,
			PathPrefixConfigured: e.PathPrefixConfigured,
		}
	}
	if h.AlertOutbox != nil {
		a := h.AlertOutbox
		posture.AlertOutbox = &site.SiteNodeAlertOutboxPosture{
			Available:                     a.Available,
			BacklogCount:                  a.BacklogCount,
			OldestQueuedOriginalMS:        a.OldestQueuedOriginalMS,
			OldestQueuedAgeMS:             a.OldestQueuedAgeMS,
			RetryIntervalMinutes:          a.RetryIntervalMinutes,
			MaxNonTierDAttempts:           a.MaxNonTierDAttempts,
			TierDCriticalWarningThreshold: a.TierDCriticalWarningThreshold,
			BatchSize:                     a.BatchSize,
		}
	}
	// h.LockoutRiskLevels intentionally excluded — keys are phone numbers.
	return posture
}

// siteExportSeverity carries the severity the runtime client read from a
// health export into the site projection. A value the client never returns is
// unknown, never ok.
func siteExportSeverity(status string) string {
	switch status {
	case runtimeclient.HealthStatusOK:
		return site.ExportSeverityOK
	case runtimeclient.HealthStatusDegraded:
		return site.ExportSeverityDegraded
	case runtimeclient.HealthStatusCritical:
		return site.ExportSeverityCritical
	default:
		return site.ExportSeverityUnknown
	}
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

// logInboundAckResult logs the outcome of a runtime's acknowledgement of a
// returned authority artifact. An unrecognised refusal reason is logged
// quoted and bounded.
func logInboundAckResult(logger *slog.Logger, err error) {
	var unrecognised *courier.UnrecognisedInboundReasonError
	switch {
	case errors.As(err, &unrecognised):
		logger.Warn("evidence inbound refusal reason unrecognised; authority artifact held until a receiver state change", "reason", unrecognised.Reason)
	case err != nil:
		logger.Warn("evidence inbound acknowledgement refused", "error", err)
	}
}

// mqttIngressView projects the refusal counts by family; Project completes
// the family set.
func mqttIngressView(ingress *broker.IngressStats) map[string]site.MQTTIngressCount {
	counts := ingress.Snapshot()
	view := make(map[string]site.MQTTIngressCount, len(counts))
	for family, count := range counts {
		view[family] = site.MQTTIngressCount{PayloadTooLarge: count.PayloadTooLarge, VolumeDropped: count.VolumeDropped}
	}
	return view
}

// stopDecision is the decision to stop (gateway-config/v2
// gateway.state_directory rule 13): one step that stops admitting work, every
// broker callback, webhook request and posture fetch, stops issuing signing
// values, and cancels the run, before anything already admitted is joined.
// It may be taken more than once, and concurrently.
type stopDecision struct {
	callbacks   *callbackGate
	webhook     *callbackGate
	posture     *postureDebouncer
	stopSigning func()
	// cancel ends the run's context, so every supervised runner stops at
	// once, not when runGateway next looks.
	cancel func()
}

func (d *stopDecision) decide() {
	d.callbacks.refuse()
	d.webhook.refuse()
	d.posture.refuse()
	d.stopSigning()
	d.cancel()
}

// callbackGate admits broker callbacks while the gateway runs and joins them
// at shutdown. refuse stops admission at the decision to stop; close also
// returns only once every callback it admitted has returned.
type callbackGate struct {
	mu      sync.Mutex
	closed  bool
	running sync.WaitGroup
}

func newCallbackGate() *callbackGate { return &callbackGate{} }

func (g *callbackGate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.running.Add(1)
	return true
}

// refuse refuses every later callback, without waiting for those running.
func (g *callbackGate) refuse() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

// refused reports whether the gate has stopped admitting: the decision to
// stop has been taken.
func (g *callbackGate) refused() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

func (g *callbackGate) close() {
	g.refuse()
	g.running.Wait()
}

// admit is enter for a caller that releases what it was admitted to.
func (g *callbackGate) admit() (release func(), admitted bool) {
	if !g.enter() {
		return nil, false
	}
	return g.running.Done, true
}

// wrap returns b with every handler it subscribes run through the gate.
func (g *callbackGate) wrap(b brokerClient) brokerClient {
	return &gatedBroker{brokerClient: b, gate: g}
}

type gatedBroker struct {
	brokerClient
	gate *callbackGate
}

func (b *gatedBroker) Subscribe(ctx context.Context, topic string, qos byte, handler broker.MessageHandler) error {
	return b.brokerClient.Subscribe(ctx, topic, qos, func(topic string, payload []byte) {
		if !b.gate.enter() {
			return
		}
		defer b.gate.running.Done()
		handler(topic, payload)
	})
}
