// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/gatewaystate"
	"github.com/ori-platform/ori-gateway/internal/heartbeat"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/site"
)

// hungMark lets the start's write through, unless holdStart, and holds every
// later write until release is closed, as a disk that stops answering would.
type hungMark struct {
	inner     mqttauth.DurableMark
	holdStart bool
	calls     atomic.Int32
	entered   chan struct{}
	once      sync.Once
	release   chan struct{}
}

func newHungMark(holdStart bool) *hungMark {
	return &hungMark{holdStart: holdStart, entered: make(chan struct{}), release: make(chan struct{})}
}

func (m *hungMark) Found() (int64, bool) { return m.inner.Found() }

func (m *hungMark) Persist(mark int64) error {
	if m.calls.Add(1) == 1 && !m.holdStart {
		return m.inner.Persist(mark)
	}
	m.once.Do(func() { close(m.entered) })
	<-m.release
	return m.inner.Persist(mark)
}

// stallingGateway is a gateway whose signing clock's mark writes hang as
// hungMark holds them, with its heartbeat's signing clock captured.
type stallingGateway struct {
	cfg     config.Config
	deps    appDependencies
	mark    *hungMark
	wallMS  *atomic.Int64
	signers chan *mqttauth.SigningClock
	exited  *atomic.Int32
}

func newStallingGateway(t *testing.T, dir string, holdStart bool) *stallingGateway {
	t.Helper()
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	g := &stallingGateway{mark: newHungMark(holdStart), wallMS: &atomic.Int64{}, signers: make(chan *mqttauth.SigningClock, 1), exited: &atomic.Int32{}}
	g.wallMS.Store(1_787_000_000_000)
	g.cfg = validConfig()
	g.cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	g.cfg.Gateway.StateDirectory = dir
	g.deps = baseDeps(t, g.cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
	g.deps.logger = slog.New(slog.DiscardHandler)
	g.deps.now = func() time.Time { return time.UnixMilli(g.wallMS.Load()) }
	g.deps.stateQuiescenceBound = 200 * time.Millisecond
	g.deps.exitProcess = func(code int) { g.exited.Store(int32(code)) }
	g.deps.signingMark = func(mark mqttauth.DurableMark) mqttauth.DurableMark {
		g.mark.inner = mark
		return g.mark
	}
	g.deps.newHeartbeat = func(_ heartbeat.PublishFunc, _ heartbeat.ProviderStatus, _ heartbeat.SIMStatus, opts heartbeat.Options) (heartbeatRunner, error) {
		g.signers <- opts.SigningClock
		return newFakeHeartbeat(), nil
	}
	t.Cleanup(func() {
		select {
		case <-g.mark.release:
		default:
			close(g.mark.release)
		}
	})
	return g
}

// stall signs past the reservation persisted at the start, so the next write
// hangs: the value is issued once the write bound opens an episode.
func (g *stallingGateway) stall(t *testing.T, signer *mqttauth.SigningClock) {
	t.Helper()
	g.wallMS.Add(10_000)
	if _, err := signer.Next(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.mark.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no write of the mark was held")
	}
	if !signer.Degraded() {
		t.Fatal("a value was issued past a held write with no episode open")
	}
}

// TestAStoppingGatewayWithAStalledWriteEndsTheProcessHoldingTheLock stops the
// gateway while a write of the mark hangs: once the quiescence bound has
// passed, the gateway ends the process with status 1 and never releases the
// state directory, so a second owner in the same process is refused even
// after the write lands (gateway-config/v2 gateway.state_directory rules 3
// and 13).
func TestAStoppingGatewayWithAStalledWriteEndsTheProcessHoldingTheLock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gateway-state")
	g := newStallingGateway(t, dir, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	g.stall(t, <-g.signers)
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stopping waited past the quiescence bound")
	}
	if g.exited.Load() != 1 || err == nil || !strings.Contains(err.Error(), "not quiescent") {
		t.Fatalf("exit status %d, runGateway = %v; want the process ended with status 1", g.exited.Load(), err)
	}
	if owner, err := gatewaystate.Acquire(dir); err == nil {
		_ = owner.Close()
		t.Fatal("a second owner acquired the state directory while a write was outstanding")
	}
	close(g.mark.release)
	time.Sleep(50 * time.Millisecond)
	if owner, err := gatewaystate.Acquire(dir); err == nil {
		_ = owner.Close()
		t.Fatal("the state directory was released in the process that ended without quiescence")
	}
}

// TestAStoppingGatewayThatReachesQuiescenceReleasesTheLock stops the gateway
// while a write hangs and lets it land within the quiescence bound: the
// process is not ended and the state directory is released.
func TestAStoppingGatewayThatReachesQuiescenceReleasesTheLock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gateway-state")
	g := newStallingGateway(t, dir, false)
	g.deps.stateQuiescenceBound = 5 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	g.stall(t, <-g.signers)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("runGateway returned (%v) with a write outstanding inside the bound", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(g.mark.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway = %v after quiescence", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runGateway did not return once the write landed")
	}
	if g.exited.Load() != 0 {
		t.Fatal("a stop that reached quiescence ended the process")
	}
	owner, err := gatewaystate.Acquire(dir)
	if err != nil {
		t.Fatalf("after a quiescent stop the state directory is still held: %v", err)
	}
	_ = owner.Close()
}

// TestAStartWhoseFirstWriteStallsIsRefusedHoldingTheLock holds the start's
// write: the start is refused once the write bound passes, no broker client is
// built, and with the write still outstanding at the quiescence bound the
// process is ended without releasing the state directory. When the write ends
// inside the bound, the refusal releases it instead.
func TestAStartWhoseFirstWriteStallsIsRefusedHoldingTheLock(t *testing.T) {
	for _, landsInTime := range []bool{false, true} {
		t.Run(fmt.Sprintf("lands in time %v", landsInTime), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "gateway-state")
			g := newStallingGateway(t, dir, true)
			var built atomic.Bool
			g.deps.newBroker = func(broker.Options) (brokerClient, error) {
				built.Store(true)
				return newFakeBroker(), nil
			}
			if landsInTime {
				g.deps.stateQuiescenceBound = 5 * time.Second
				go func() {
					<-g.mark.entered
					time.Sleep(mqttauth.MarkWriteBound + 200*time.Millisecond)
					close(g.mark.release)
				}()
			}
			err := runGateway(t.Context(), "gateway.yaml", g.deps)
			var stalled *mqttauth.StartWriteError
			if !errors.As(err, &stalled) {
				t.Fatalf("runGateway = %v, want the start refused for its stalled write", err)
			}
			if built.Load() {
				t.Fatal("a broker client was built after the start was refused")
			}
			owner, acquireErr := gatewaystate.Acquire(dir)
			if acquireErr == nil {
				_ = owner.Close()
			}
			switch {
			case landsInTime && (g.exited.Load() != 0 || acquireErr != nil):
				t.Fatalf("exit status %d, Acquire = %v; want the refusal to release the directory", g.exited.Load(), acquireErr)
			case !landsInTime && (g.exited.Load() != 1 || acquireErr == nil):
				t.Fatalf("exit status %d, Acquire = %v; want the process ended holding the directory", g.exited.Load(), acquireErr)
			}
		})
	}
}

// TestSiteHealthReportsAnEpisodeWithoutWaitingOnTheWrite holds a write of the
// mark until an episode opens: site health reports the gateway degraded at
// once, although the write is still outstanding, and healthy again once a
// write covers every value issued.
func TestSiteHealthReportsAnEpisodeWithoutWaitingOnTheWrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "gateway-state")
	g := newStallingGateway(t, dir, false)
	g.cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: loopbackAddr(t)}
	g.deps.loadConfig = func(string) (config.Config, error) { return g.cfg, nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	t.Cleanup(func() { cancel(); <-done })
	g.stall(t, <-g.signers)
	status := func() string {
		resp, err := http.Get("http://" + g.cfg.SiteHealth.ListenAddr + "/health") //nolint:noctx
		if err != nil {
			return ""
		}
		defer resp.Body.Close()
		var health site.SiteHealth
		_ = json.NewDecoder(resp.Body).Decode(&health)
		return health.Gateway.Status
	}
	waitUntil(t, "site health degraded", func() bool { return status() == site.SiteStatusDegraded })
	close(g.mark.release)
	waitUntil(t, "site health healthy", func() bool { return status() == site.SiteStatusHealthy })
}

// TestAStateDirectoryRefusalCreatesNoEvidenceQueue starts the gateway with
// evidence enabled on a state directory whose mark is corrupt: the start is
// refused before either evidence queue directory is created.
func TestAStateDirectoryRefusalCreatesNoEvidenceQueue(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	dir := t.TempDir()
	state := filepath.Join(dir, "gateway-state")
	owner, err := gatewaystate.Acquire(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, ".ori-signed-at-messages"), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.StateDirectory = state
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: filepath.Join(dir, "outbound"),
		ReturnQueueDirectory: filepath.Join(dir, "returned"), MaxItems: 10,
		MaxBytes: 8 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
	}
	deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.logger = slog.New(slog.DiscardHandler)
	var built atomic.Bool
	deps.newBroker = func(broker.Options) (brokerClient, error) {
		built.Store(true)
		return newFakeBroker(), nil
	}
	if err := runGateway(t.Context(), "gateway.yaml", deps); err == nil || !strings.Contains(err.Error(), "gateway state directory") {
		t.Fatalf("runGateway = %v, want the state directory refused", err)
	}
	for _, queue := range []string{cfg.Evidence.QueueDirectory, cfg.Evidence.ReturnQueueDirectory} {
		if _, err := os.Lstat(queue); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("evidence queue %s exists after a refused start: %v", filepath.Base(queue), err)
		}
	}
	if built.Load() {
		t.Fatal("a broker client was built after the state directory refused")
	}
}

const (
	stateChildMode = "ORI_GATEWAY_STATE_CHILD"
	stateChildDir  = "ORI_GATEWAY_STATE_CHILD_DIR"
)

// TestStateDirectoryChild is the gateway process TestAStalledWriteHoldsTheLockUntilTheProcessEnds
// runs: it stalls a write of the mark through runGateway, at a stop or at the
// start as its mode says, and ends its process with the status runGateway
// chooses, once its parent has closed its standard input.
func TestStateDirectoryChild(t *testing.T) {
	mode := os.Getenv(stateChildMode)
	if mode == "" {
		t.Skip("runs only as the child process of TestAStalledWriteHoldsTheLockUntilTheProcessEnds")
	}
	g := newStallingGateway(t, os.Getenv(stateChildDir), mode == "start")
	g.deps.exitProcess = func(code int) {
		fmt.Println("exiting")
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	if mode == "stop" {
		select {
		case signer := <-g.signers:
			g.stall(t, signer)
		case err := <-done:
			t.Fatalf("runGateway = %v before the heartbeat was built", err)
		}
		cancel()
	}
	err := <-done
	cancel()
	// Reaching here means runGateway returned instead of ending the process.
	fmt.Printf("returned: %v\n", err)
	os.Exit(0)
}

// TestAStalledWriteHoldsTheLockUntilTheProcessEnds runs the gateway in a child
// process whose write of the mark hangs, at a stop and at the start: while the
// child ends its process, another process is refused the state directory, the
// child exits with status 1, and its exit releases the directory
// (gateway-config/v2 gateway.state_directory rules 3, 13 and 14, on a local
// filesystem with flock).
func TestAStalledWriteHoldsTheLockUntilTheProcessEnds(t *testing.T) {
	if os.Getenv(stateChildMode) != "" {
		t.Skip("the parent test does not run inside its own child")
	}
	for _, mode := range []string{"stop", "start"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "gateway-state")
			cmd := exec.Command(os.Args[0], "-test.run=^TestStateDirectoryChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), stateChildMode+"="+mode, stateChildDir+"="+dir)
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = io.Discard
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
			lines := make(chan string, 16)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					lines <- scanner.Text()
				}
				close(lines)
			}()
			deadline := time.After(20 * time.Second)
		wait:
			for {
				select {
				case line, ok := <-lines:
					if !ok {
						t.Fatal("the child ended without deciding to end its process")
					}
					if line == "exiting" {
						break wait
					}
					if strings.HasPrefix(line, "returned") {
						t.Fatalf("the child's runGateway %s instead of ending the process", line)
					}
				case <-deadline:
					t.Fatal("the child never decided to end its process")
				}
			}
			if owner, err := gatewaystate.Acquire(dir); err == nil {
				_ = owner.Close()
				t.Fatal("another process acquired the state directory while the child's write was outstanding")
			}
			_ = stdin.Close()
			err = cmd.Wait()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("the child ended with %v, want status 1", err)
			}
			owner, err := gatewaystate.Acquire(dir)
			if err != nil {
				t.Fatalf("the child's exit did not release the state directory: %v", err)
			}
			_ = owner.Close()
		})
	}
}
