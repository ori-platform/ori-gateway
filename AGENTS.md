# AGENTS.md - ori-gateway

This repository implements the LAN gateway and site coordinator for Ori.

## Purpose

`ori-gateway` has four responsibilities:

1. Provide Tier 3 LAN reasoning for [`ori-runtime`](https://github.com/ori-platform/ori-runtime) devices.
2. Publish a LAN health heartbeat for [`ori-runtime`](https://github.com/ori-platform/ori-runtime) capability posture.
3. Coordinate multi-device site context.
4. Act as the blind, durable courier between runtimes and the independent evidence authority.

It is not [`ori-runtime`](https://github.com/ori-platform/ori-runtime) and not [`ori-cloud`](https://github.com/ori-platform/ori-cloud).

## Invariants

Each invariant below is a summary. Its full, binding text, with the tests
that verify it, is the section of the same name in
[`docs/INVARIANTS.md`](docs/INVARIANTS.md); where the two differ, that text
governs. Before changing code an invariant covers, read its full text there,
and change both files together.

1. `GW-1` Topic names must match [`ori-specs/gateway-api/v2.md`](https://github.com/ori-platform/ori-specs/blob/main/gateway-api/v2.md) exactly.

2. `GW-2` Every response must echo the request `request_id`, on provider
timeout, error, panic, invalid response and publish failure too, whenever the
request is valid enough to correlate. A missing or changed `request_id` causes
runtime timeout and fallback.

3. `GW-3` Providers must satisfy one shared interface. Provider-specific
settings belong in config. Tier 3 reasoning providers are selected through the
reasoning provider factory only; customer-reporting providers never are.

4. `GW-4` Gateway never changes action authority. [`ori-runtime`](https://github.com/ori-platform/ori-runtime) owns action-tier
authority; the gateway may echo tier hints but must not promote or downgrade
physical authority.

5. `GW-5` Gateway is not in the Tier D path. Tier D safety fires locally in the
runtime rule engine before gateway, cloud, or network systems are consulted;
keep Tier D evaluation and actuation APIs out of this repository.

6. `GW-6` Heartbeat must be reliable. If heartbeat publication cannot
continue, the gateway reports failure or exits rather than silently appearing
available, and repeated reasoning-reply delivery failures
(`dispatcher.ErrPublishResponse` only) end it. A refused, malformed,
unauthenticated, stale or already-in-flight request neither counts nor resets
the count, and nothing the run's own stop interrupts or refuses is a failure.
No escalation waits on the log destination: the gateway's components share
one `internal/asynclog` queue, and exit lines get a bounded flush
(`asynclog.ExitFlushBound`, 1 s). Site health reports the gateway `degraded`
while its heartbeat is not going out.

7. `GW-7` Fleet/cloud forwarding is opt-in. With fleet disabled, the gateway
performs zero HTTP, DNS, auth, credential, or background cloud work.

8. `GW-8` Site coordination is LAN-scoped. Cross-device correlation and shared
GSM are site functions; fleet analytics and billing belong in `ori-cloud`.

9. `GW-9` Gateway must not persist reasoning results. Stateful learning and
causal memory belong in runtime or cloud-defined stores, never in the gateway
request path.

10. `GW-10` Product reporting providers are separate from Tier 3 reasoning
providers: reporting-specific config, never wired into the Tier 3 reasoning
provider factory.

11. `GW-11` Gateway reporting and enrichment never change action authority.
Weekly reports and Tier C enrichment are advisory and must not promote,
downgrade, approve, reject, bypass, or execute runtime action tiers. Weekly
reports consume bounded runtime exports through `runtimeclient.Client`, never
runtime SQLite, and never leak remote-command sender identities, credentials,
MQTT URLs, filesystem paths, or lockout risk details.

12. `GW-12` Reporting provider credentials stay out of runtime config. Only
environment variable names appear in gateway config; secret values are never
committed, and runtime config examples stay provider-neutral.

13. `GW-13` SIM access is opt-in. With SIM disabled, the gateway never
initialises, opens, probes, or otherwise touches modem or serial hardware.

14. `GW-14` Gateway data access goes through runtime-owned interfaces. The
gateway never reads runtime SQLite files; runtime data, posture fields
included, comes from runtime export interfaces. A health export's severity is
read once, in `runtimeclient.MQTTClient.Health` after the export verifies, and
every consumer reads `HealthSnapshot.Status` (`ok`, `degraded`, `critical`,
`unknown`): critical is never read as less, and an absent, unknown, mistyped
or inconsistent member reads `unknown`, never `ok`.

15. `GW-15` Gateway process startup and shutdown must be ordered and explicit.
Startup order is config, the gateway state directory (with `gateway.auth`),
the evidence stores (with evidence), provider, broker connect, optional
modules, heartbeat, dispatcher, then MQTT subscribe; a failed required stage starts
nothing later. Long-running tasks run under the shared `errgroup` supervisor.
The decision to stop is one step: it refuses every later broker callback,
webhook request and posture fetch, stops the signing clock and cancels the
run; admitted work is then joined. The state directory is released only once
no mark write is outstanding; at the 5-second quiescence bound the process
exits with status 1 holding it.

16. `GW-16` Public SMS provider ingress must be signed before it reaches
runtime. The webhook bridge validates source CIDRs, caps the body, preserves
the raw body, signs it with `X-Ori-Webhook-*` HMAC headers from
environment-sourced secrets, and forwards only to the runtime's local webhook
URL. It bounds volume per sender and bridge-wide (429, never forwarded) and
never decides authority. Nothing it exposes carries target URLs, env var
names, CIDRs, SMS bodies, phone numbers, bearer tokens, or HMAC secrets.

17. `GW-17` Runtime export clients must match runtime MQTT security posture.
When `gateway.auth.enabled=true`, export requests must be HMAC-signed and
export responses must be verified with current-or-previous gateway secrets.
When `gateway.encryption.enabled=true`, sensitive runtime export responses
(`sensor_history`, `action_log`, `reasoning_log`, `tier_c_decision_log`) must
be AES-GCM decrypted after HMAC verification. Health exports may remain
plaintext but still authenticated. New outbound signatures must use the current
secret only; previous secrets are verify-only.

18. `GW-18` Site health projection is read-only, advisory, and secrets-free.
`internal/site` never reads runtime SQLite or config, never changes action
authority, never imports `internal/runtimeclient`, and exposes no secrets,
URLs, env var names, phone numbers, paths or trigger names. Severity is never
folded: `critical` stays `critical`, and a node silent after a critical
heartbeat or export is retained. Liveness, eviction and staleness are judged
on the gateway's monotonic receipt clock, never a wall clock, against one
fixed 90-second TTL, `site.NodeTTL`, never derived from `heartbeat_interval_s`.
The optional `site_health` server exposes `GET /health` on a loopback address
(default `127.0.0.1:8765`); when `site_health.enabled=false`, no socket is
opened and no goroutine is started.

19. `GW-19` The gateway is a blind, durable evidence courier. Evidence is
queued byte-for-byte before custody is acknowledged; custody is never an
authority receipt; signed artifacts are never verified, rewritten or
re-signed; and evidence is never discarded. Delivery is ordered per device and
lane, refusals are decided only in `refusal.Policy`, and an authority artifact
retires only on an authenticated acknowledgement from its own device. The
courier is opaque to artifact versions and never rejects, holds, retires or
classifies an artifact by its declared `v`; an artifact over 1,048,576 bytes
is refused `malformed` at admission, before custody. The
evidence channel shares nothing with fleet, and transport errors are reduced
before logs or status.

20. `GW-20` Contract vectors are vendored, and an unmet case is pinned, never
skipped. Tests read `ori-specs` vectors only through `internal/specvectors`,
pinned by its `MANIFEST.json`. No issue or pull request number appears
anywhere but a pin's `Issue` field.

21. `GW-21` MQTT ingress is bounded before it is read. Every subscription
belongs to a gateway-api/v2 Payload Size family; the broker wrapper checks
length, then the device's message and byte rate, before any handler, and a
refused payload is never parsed or answered. No setting changes a maximum or
rate. Every required reply fits the 4096-byte floor.

22. `GW-22` A runtime-facing payload has one reading. Every runtime payload,
and every decrypted export plaintext, is admitted under gateway-api/v2's
Payload Member Rule (`contracts.AdmitPayload`) before it is read; a refused one
changes nothing and is never published.

23. `GW-23` An export exchange ends only on a verified response or its bound.
With auth on, an export response is verified under its waiting request before
anything is delivered; one that is not is dropped and leaves the request
waiting. Every export wait, pacing included, is bounded inside the client, and
no logger holds it.

24. `GW-24` Envelope freshness and replay follow gateway-api/v2. Envelopes are
refused stale, future, below the replay floor, then replay, each under its own
error. Every runtime-gateway envelope outside the evidence path is signed from
the one shared `mqttauth.SigningClock`, whose mark lives in
`gateway.state_directory`, owned by `internal/gatewaystate` alone.

## Verification

Run the gate steps CI runs (`.github/workflows/ci.yml` is authoritative), each
on its own:

```bash
python3 scripts/check_workflows.py && python3 scripts/test_check_workflows.py   # needs PyYAML (.venv)
go mod verify && (cd tools && go mod verify)
test -z "$(gofmt -l .)"
go vet ./...
SKIP=no-commit-to-branch pre-commit run --all-files
scripts/lint.sh
go test ./...
```

CI also runs `go test -race ./...`. While editing, run only the packages a
change touches.

---

## Supply Chain Security Invariants

These rules apply to every AI coding agent modifying this repository.
The gateway proxies reasoning requests between physical devices and cloud LLMs —
supply chain integrity here has direct impact on device actuation decisions.

1. Never add `pull_request_target` workflows that checkout or execute untrusted
   PR code. Use `pull_request` for fork PR workflows.

2. Every GitHub Actions workflow must declare explicit least-privilege
   permissions. Normal CI uses `contents: read` and `id-token: none`.

3. `id-token: write` is allowed only in a dedicated release job in `release.yml`.
   It must never appear in `ci.yml`.

4. Release jobs must never restore dependency caches. Cache poisoning was a key
   vector in the TanStack May 2026 supply-chain attack. `setup-go` must have
   `cache: false` in any job that has publish permissions.

5. GitHub Actions must be pinned to full commit SHAs. Mutable tags such as
   `@v4`, `@v5`, `@main` are forbidden. Add a human-readable version comment.

6. Never download and execute remote scripts in CI without hash verification.
   `curl URL | bash` and equivalent patterns are forbidden.

7. `GOFLAGS=-mod=readonly` must be set in all CI jobs. This prevents Go from
   implicitly updating `go.mod` or `go.sum` during CI runs.

8. `go mod verify` must run before any build or test step. This verifies the
   integrity of the module cache against `go.sum` checksums.
   `scripts/check_workflows.py` holds rules 2, 7 and 8 for every job:
   - each workflow declares `contents: read` and `id-token: none`, and so does
     any job that sets its own permissions, except in `release.yml` (rule 3);
   - each job sets `GOFLAGS=-mod=readonly` in its env and sets no
     `defaults`. `GOFLAGS` and `CGO_ENABLED` are set only as keys of a job's
     env; any other mention of either, in any case and anywhere but a job's or
     step's display name, is refused, even a harmless one such as
     `go env GOFLAGS`. The one exception is the run text of the root-only step
     in `ci.yml`'s `test` job, pinned exactly, which passes both as constants
     through `sudo env`;
   - every env mapping (job, step, container, service), and every mapping that
     holds one, is literal: no key anywhere in a job is an expression, and no
     container, service, services block or step is computed (a container may
     be a literal image name);
   - each job's first command is exactly `go mod verify` or
     `go mod verify && (cd tools && go mod verify)`, in a step that sets only
     `name` and `run`.

   The guard is a fail-closed policy on how a workflow is written. It parses no
   shell and does not prove that a command cannot change the environment. It
   leaves these to review: an action run before the verify step, a setting
   changed at run time (`$GITHUB_ENV`, `go env -w`, a script, or a command
   computed from an expression such as `run: ${{ vars.CMD }}`), a command-line
   flag such as `go build -mod=mod`, and the runner's `PATH` that the pinned
   root step carries into root.

9. Every job in every workflow must set `CGO_ENABLED=0` in its job `env`, and
   nothing in the job may set it to anything else. The gateway has no CGO
   dependency. No workflow sets `env` or `defaults` at workflow level, since
   both reach every job. The one exception is the `race` job in `ci.yml`: the
   race detector needs cgo, so that job sets `CGO_ENABLED=1`. It runs on
   `ubuntu-latest`, and its `env` is exactly that, `GOFLAGS: -mod=readonly`
   and an empty `GONOSUMCHECK`. It sets no keys but `name`, `runs-on`, `env`
   and `steps`, so it has no permissions of its own, and its steps set only
   `name`, `uses`, `with` and `run`. It uses only the SHA-pinned harden-runner,
   checkout and setup-go actions, with their listed inputs, and runs exactly
   `go mod verify`, then `go test -race ./...`. `ci.yml` must keep the job.
   `scripts/check_workflows.py` checks these declared settings per job. It
   reads each workflow as YAML with PyYAML, from the hash-locked CI
   requirements. It refuses what it cannot parse, and what GitHub could read
   differently: duplicate keys, `<<` merge keys and explicit tags. It also
   refuses a job that calls a reusable workflow and a step that uses a local
   action, since their steps are outside it. It cannot read what a shell command does at run
   time; a script that writes `CGO_ENABLED` to `$GITHUB_ENV`, for example, is
   left to review.

10. `go.sum` must be committed and kept up to date. Never bypass sum verification.

11. Run `scripts/check_workflows.py` before merging workflow or pre-commit
    configuration changes. The script fails on forbidden patterns.

## Review before handoff

Self-review before handing off work is **required, and is never sufficient
proof.** State plainly what remains unverified: what was simulated, deferred,
tested on the host only, or left dependent on another repository or on hardware.
A handoff that lists only successes misrepresents its own coverage.

It does not replace independent review for shared contracts, Tier D or
physical-authority changes, release and install work, or any claim of HIL proof.

The method is the `ori-rigorous-review` skill, kept in the `ori-specs`
repository under `agent-skills/` and installed with
`scripts/install-agent-skills`. Read it before reviewing; do not reimplement it
here.
