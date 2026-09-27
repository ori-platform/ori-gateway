# AGENTS.md - ori-gateway

This repository implements the LAN gateway and site coordinator for Ori.

## Purpose

`ori-gateway` has three responsibilities:

1. Provide Tier 3 LAN reasoning for [`ori-runtime`](https://github.com/ori-platform/ori-runtime) devices.
2. Publish a LAN health heartbeat for [`ori-runtime`](https://github.com/ori-platform/ori-runtime) capability posture.
3. Coordinate multi-device site context.
4. Act as the blind, durable courier between runtimes and the independent evidence authority.

It is not [`ori-runtime`](https://github.com/ori-platform/ori-runtime) and not [`ori-cloud`](https://github.com/ori-platform/ori-cloud).

## Invariants

1. `GW-1` Topic names must match [`ori-specs/gateway-api/v1.md`](https://github.com/ori-platform/ori-specs/blob/main/gateway-api/v1.md) exactly.

2. `GW-2` Every response must echo the request `request_id`.
A missing or changed `request_id` causes [`ori-runtime`](https://github.com/ori-platform/ori-runtime) timeout and fallback behavior.
Provider timeout, provider error, provider panic, invalid provider response, and
publish-failure paths must still preserve the original `request_id` whenever the
incoming request is valid enough to correlate. Verified by:
`TestDispatcherTimeoutErrorResponse`, `TestDispatcherProviderErrorResponse`,
`TestDispatcherProviderPanicResponse`,
`TestDispatcherProviderInvalidResponsePublishesErrorResponse`, and
`TestDispatcherPublishFailureIsSurfaced`.

3. `GW-3` Providers must satisfy one shared interface.
Provider-specific settings belong in config, not in the provider interface.
Tier 3 reasoning providers are selected through the reasoning provider factory
only. Customer-reporting providers must not be routed through that factory.
Verified by `TestReportingProviderDoesNotAffectReasoningProvider`.

4. `GW-4` Gateway never changes action authority.
[`ori-runtime`](https://github.com/ori-platform/ori-runtime) owns action-tier authority. Gateway may echo tier hints but must not
promote or downgrade physical authority.

5. `GW-5` Gateway is not in the Tier D path.
Tier D safety fires locally in the [runtime](https://github.com/ori-platform/ori-runtime) rule engine before gateway, cloud, or
network systems are consulted.
No direct test currently enforces this; it is enforced by code review and by
keeping Tier D evaluation and actuation APIs out of this repository.

6. `GW-6` Heartbeat must be reliable.
If heartbeat publication cannot continue, the gateway must report failure or
exit rather than silently appearing available.
The gateway process must also surface repeated reasoning-response delivery
failures instead of logging forever while dropping all replies. Verified by
`TestHeartbeatFailureLimit`, `TestGatewayHeartbeatFailureReturnsError`, and
`TestMainEscalatesRepeatedRequestHandlerFailures`.

7. `GW-7` Fleet/cloud forwarding is opt-in.
With fleet disabled, gateway must not call `ori-cloud`, resolve cloud hosts, or
attempt authentication.
Disabled fleet must perform zero HTTP, DNS, auth, credential, or background
cloud work. Verified by `TestDisabledFleetNoNetwork` and
`TestMainDisabledOptionalModulesDoNotFailStartup`.

8. `GW-8` Site coordination is LAN-scoped.
Cross-device correlation and shared GSM are site functions. Fleet analytics and
billing belong in `ori-cloud`.

9. `GW-9` Gateway must not persist reasoning results.
Stateful learning and causal memory belong in [runtime](https://github.com/ori-platform/ori-runtime) or cloud-defined stores, not
in the gateway request proxy.
No direct test currently enforces this; it is enforced by code review and by
keeping durable reasoning stores out of the gateway request path.

10. `GW-10` Product reporting providers are separate from Tier 3 reasoning providers.
The gateway may own connected customer-reporting and enrichment providers, but
those providers must use reporting-specific config and must not be wired into
the Tier 3 reasoning provider factory.
Verified by `TestReportingProviderDoesNotAffectReasoningProvider`.

11. `GW-11` Gateway reporting and enrichment never change action authority.
Customer-facing weekly reports and Tier C explanation enrichment are advisory.
They must not promote, downgrade, approve, reject, bypass, or execute runtime
action tiers. Runtime remains the physical action authority. Tier C enrichment
is verified by `TestTierCEnrichmentCannotChangeActionAuthority` and
`TestTierCEnrichmentDropsInjectedAuthorityFields`.
Weekly report generation must consume bounded runtime exports through
`runtimeclient.Client` and must not read runtime SQLite directly. Runtime posture
may be surfaced as non-secret customer-facing warnings, but reports must not leak
remote-command sender identities, credentials, MQTT URLs, filesystem paths, or
lockout risk details. Report output may be delivered or persisted by
product/cloud layers, not by mutating runtime state. Weekly report boundaries are
verified by
`TestWeeklyReportBuildsInputFromRuntimeExports`,
`TestGatewayConstructsSecureRuntimeClientForWeeklyReports`, and
`TestWeeklyReportRunnerLogsFailureAndContinues`.

12. `GW-12` Reporting provider credentials stay out of runtime config.
Gemini/API keys and equivalent product-provider credentials belong in gateway
or product environment variables only. Secret values must never be committed,
and runtime config examples must remain provider-neutral.
Only environment variable names may appear in gateway config. Verified by
`TestReportingAPIKeyEnvMustBeEnvVarName`.

13. `GW-13` SIM access is opt-in.
With SIM disabled, gateway must not initialise, open, probe, or otherwise touch
modem or serial hardware. Verified by `TestDisabledSIMNoSerialProbe` and
`TestMainDisabledOptionalModulesDoNotFailStartup`.

14. `GW-14` Gateway data access goes through runtime-owned interfaces.
Gateway must not read runtime SQLite files directly. Sensor history, action
logs, Tier C decisions, and runtime health must be consumed through explicit
runtime-owned export interfaces. Runtime health posture fields such as broker
hardening, state-store encryption, and alert outbox backlog must be mapped from
the health export rather than inferred by reading runtime files. Current
enforcement is structural: the gateway has no SQLite driver dependency and
runtime-client tests such as `TestTierCDecisionLogRequestValidation` validate
the export request contract. Future transport tests must preserve this boundary
and must not add direct SQLite access.

15. `GW-15` Gateway process startup and shutdown must be ordered and explicit.
Startup order is config, provider, broker connect, optional modules, heartbeat,
dispatcher, then MQTT subscribe. Failure at any required stage must prevent
later stages from starting. Shutdown must cancel background work and disconnect
the broker. Long-running app tasks must be supervised through the shared
`errgroup`-backed supervisor rather than open-coded result channels, so adding a
new runner does not require updating every early-return drain path by hand.
Verified by `TestMainStartupMissingConfig`,
`TestGatewayStartupProviderFailureStopsBeforeBroker`,
`TestGatewayStartupConnectFailureStopsBeforeOptionalModules`,
`TestGatewayStartupSubscribeFailureCancelsHeartbeatAndDisconnects`,
`TestMainStartsHeartbeatBeforeSubscribe`, `TestGracefulShutdown`,
`TestSupervisedRunnersReportsRunnerErrors`,
`TestSupervisedRunnersWaitsForAllRunners`, and
`TestSupervisedRunnersUnexpectedNilReturnIsError`.

16. `GW-16` Public SMS provider ingress must be signed before it reaches runtime.
Providers such as Africa's Talking do not emit Ori HMAC headers. The gateway
webhook bridge is the allowed production adapter for that case: it validates
source CIDRs, caps request body size, preserves the raw provider body, adds
`X-Ori-Webhook-*` HMAC headers using environment-sourced secrets, and forwards
only to the runtime's local webhook URL. Gateway heartbeat may expose bridge
readiness and limit posture, but `ready` must reflect bridge-loop liveness rather
than static config. It must never expose target URLs, env var names, provider
CIDR values, SMS body content, phone numbers, bearer tokens, or HMAC secrets.

17. `GW-17` Runtime export clients must match runtime MQTT security posture.
When `gateway.auth.enabled=true`, export requests must be HMAC-signed and
export responses must be verified with current-or-previous gateway secrets.
When `gateway.encryption.enabled=true`, sensitive runtime export responses
(`sensor_history`, `action_log`, `reasoning_log`, `tier_c_decision_log`) must
be AES-GCM decrypted after HMAC verification. Health exports may remain
plaintext but still authenticated. New outbound signatures must use the current
secret only; previous secrets are verify-only.

18. `GW-18` Site health projection is read-only, advisory, and secrets-free.
`internal/site.Projector` computes a `SiteHealth` snapshot from the node registry
and a caller-supplied `GatewayView`. It must not read runtime SQLite files, read
runtime config, or change action authority. `ActiveTriggers` from node heartbeats
are represented only as a count (`ActiveTriggerCount int`) to prevent trigger-name
strings from leaking. `GatewayView` must not contain target URLs, MQTT URLs, env
var names, bearer tokens, HMAC secrets, phone numbers, or filesystem paths.
Consumers depend on the `site.Viewer` interface, not on MQTT or registry internals.
The optional `site_health` HTTP server (`HealthHandler`) exposes `GET /health`
on a loopback address (default `127.0.0.1:8765`) and must not appear in the
JSON output. When `site_health.enabled=false`, no socket is opened and no goroutine
is started.
`internal/site` must never import `internal/runtimeclient`. Posture types are
defined independently in `internal/site` (`SiteNodePosture`, `SiteNodeBrokerPosture`,
`SiteNodeEncryptionPosture`, `SiteNodeAlertOutboxPosture`); mapping from
`runtimeclient.HealthSnapshot` to `site.SiteNodePosture` is done exclusively in
`cmd/ori-gateway/app.go` (`sitePostureFromHealth`). `LockoutRiskLevels` from the
health snapshot is intentionally excluded — its map keys are phone numbers.
Verified by `TestProjectSiteHealthAllNodesHealthy`,
`TestProjectSiteHealthStaleNode`, `TestProjectSiteHealthMissingNode`,
`TestProjectSiteHealthGatewayDegradedStatus`,
`TestProjectSiteHealthDisabledWebhookBridgeIsInert`,
`TestProjectSiteHealthActiveTriggerCountNotStrings`,
`TestProjectSiteHealthNoSecretsOrURLsInJSON`,
`TestProjectSiteHealthFutureDatedNodeIsStale`,
`TestProjectSiteHealthNodePosturePassedThrough`,
`TestProjectSiteHealthNodeWithNoPostureOmitsField`,
`TestProjectSiteHealthPostureLockoutRiskLevelsNeverInOutput`,
`TestHealthHandlerGETReturnsJSON`, `TestHealthHandlerMethodNotAllowed`,
`TestHealthHandlerProjectionStatusInBody`, `TestHealthHandlerNoSecretsInResponse`,
`TestHealthHandlerRunStartsAndStopsCleanly`, `TestHealthHandlerRunBindFailure`,
`TestGatewaySiteHealthServerStartsWhenEnabled`,
`TestGatewaySiteHealthDisabledDoesNotStartServer`,
`TestGatewaySiteHealthConstructsRuntimeClientForPosture`, and
`TestGatewaySiteHealthPostureFetchedAfterHeartbeat`.

19. `GW-19` The gateway is a blind, durable evidence courier.
Outbound evidence artifacts must be queued byte-for-byte before custody is
acknowledged, and queue exhaustion must be an explicit retriable refusal.
Custody is authenticated under the dedicated `gateway_custody` secret and must
never be treated as an authority receipt. Delivery envelopes, registrations,
commissioning authorisations, and checkpoints remain end-to-end signed; the
gateway does not verify, rewrite, or re-sign them. Authority receipts and epoch
confirmations remain opaque and must be durably staged before outbound queue
retirement. The evidence channel shares no authority, keys, storage,
acknowledgement, or failure status with fleet management, and transport errors
must be reduced before reaching logs or status so the evidence authority's
identity and endpoint cannot leak.

Delivery order is per device and lane (`evidence-transport/v2`, refusal
policy). Each device has two lanes, its anchor registrations and every other
artifact it delivers; a record's lane is derived from its carriage artifact
type and never stored, so queue directories need no migration. Order, retry
state, back-off and holds are kept, persisted and restored per `device_id` and
lane, and a hold or back-off in one lane never delays the device's other lane
or another device. A handoff is a newly admitted artifact for the same device
(a handoff for both its lanes), a `200` retiring an anchor registration for
the same device, confirmed or accepted pending (a handoff for its evidence
lane), or the courier starting again; it retries only a handoff-only refusal
and never brings a back-off forward. Queue capacity is reserved per configured
device, as an equal share of `max_items` and `max_bytes`: a device past its own
share is refused `queue_full` at admission and cannot exhaust another's. Each
share reserves one item and one maximum encoded registration record for
registrations only; the evidence lane is refused `queue_full` before it would
take that reserve. Each share must hold two items and one maximum encoded
registration record plus one maximum encoded evidence record, refused at load
when a device declares `gateway-evidence-carriage/v1` in
`evidence.device_carriage`; a site that declares none keeps the courier's own
validation (`gateway-config/v2`). A terminal refusal is held in either lane:
without a verified retention nothing is archived (`evidence-transport/v2`).
Verified-retention archival, incidents and stopped custody are not implemented;
their contract cases run as pinned expected failures. Lane capacity, lane head
selection and the retirement handoff are each decided in one place
(`laneShareRefusalLocked`, `deliveryLane.head`, `deliveryLane.retired`). A
`gateway.device_ids` entry carrying a control character (Cc, NUL among them) is
always refused at load; an entry that declares `gateway-evidence-carriage/v1`
must also be in the evidence routing domain. A carriage whose topic names an unconfigured
device is rejected before admission: nothing is admitted, no custody is issued
and no acknowledgement is published, so the runtime keeps its bytes and
retries; `queue_full` is only ever a configured device's full share. An artifact over 1,048,576 bytes is refused
`malformed` at admission, before custody. The courier is opaque to artifact
versions: it reads only the routing projection (`device_id`; `local_seq` of an
envelope; `from_seq` and `to_seq` of a returned receipt), each held to its v1
domain from `evidence-exchange/v1`, and never rejects,
holds, retires or classifies an artifact by its declared `v`, so a version it
does not know is admitted and delivered and the evidence authority alone decides
version support. Verified by `TestAVersionTheCourierDoesNotKnowIsAdmittedAndDelivered`.

A head the evidence authority refuses terminally is held and never discarded,
and the running process never re-sends it. The hold is persisted beside its
queue record, validated on load, and restored at startup without a delivery
attempt. The bound is per process: each process that starts without a durable
hold — because an earlier one died, or its hold write failed, between the
refusal and the durable hold — sends the head once and is refused again; once a
hold is durably written, no later start sends it. A back-off is persisted the
same way, so a restart never resends a backed-off head early. Evidence is never
discarded in any case.

What a refusal does is decided in one place, `refusal.Policy`, which is the
refusal policy table of `ori-specs/evidence-transport/v2.md`; nothing about
this hop is taken from `gateway-api/v1`. The `retriable` flag is never used to
select a well-formed refusal's class. `400` holds; `409` holds, except
`409 pending_registration_conflict`, which is retained and backed off; `401` and `403` are
retried on the next handoff only, never on a timer; `422` holds for
`bad_authenticator`, `binding_mismatch` and `commissioning_digest_mismatch` and
is handoff-only for `unrecognised_version` and `unknown_key` (`422 malformed`
is invalid authority output and holds as `unrecognised`); `429` and `503` back
off, and a `429 rate_limited` or `429 pending_registration_limit` waits no
less than its `Retry-After`. A `refused_retained` response and a `507` are not
yet handled and back off as unrecognised; their cases run as pinned expected
failures. When a device declares `gateway-evidence-carriage/v1`, back-off
runs from `backoff_base_s` to `backoff_max_s`, refused at load unless
`backoff_max_s >= backoff_base_s >= retry_interval_s`; a site that declares
nothing does not consume those keys and backs off from the delivery interval
to 300 seconds (or the delivery interval, if longer). Every wait is at least the
delivery interval, and a `Retry-After` is an additional floor. Each
status admits only its own closed reason list: a reason outside it is recorded
as `unrecognised` and takes the status's fail-closed action. Anything else — a
status outside the table, a response whose media type, body, fields, digest
binding, outcome or retry metadata is invalid or absent, a `429 rate_limited`
or `429 pending_registration_limit` without `Retry-After`, any `2xx` that is not a clean `200` acceptance — is an
unrecognised outcome: retained, recorded `unrecognised`, reported degraded and
backed off exponentially within a bound, never retired, never held, never
resent immediately. Only an admitted reason reaches a log or status.

Site health projects `evidence_delivery.devices` exactly as `gateway-evidence-carriage/v1`
states: one entry per device and lane that is not clean (`held`,
`waiting_handoff`, `backing_off`), each carrying its `lane`, no two sharing a
`device_id` and `lane`, `held` present exactly when the state is `held`,
`blocked` true exactly when pending is positive, every pending artifact is in a
listed lane and every listed lane is held, and `degraded` true exactly when
`faults` or `devices` is not empty. `incidents` and `stopped_custody` are not
yet projected. `faults` is always carried, and a fault never makes `blocked` true:
`store_unavailable` per durable store, raised by any failed store operation and
by a failed store probe, and cleared only by that store's next successful
probe, never by a successful operation; `admission_failed` per admission form (durable outbound queueing, and
the durable staging of an envelope's custody return), cleared only by a later
completed admission of the same form and never by a probe, and never raised by
a failure to publish the acknowledgement after durable admission;
`device_unconfigured` when a carriage names an unconfigured device, persisted
without the device ID ever being projected, and cleared only when that device
is configured and a later handoff for it is admitted. `delivery_impaired` is
never raised, because `devices` is always projected and complete. Each store is
probed right after opening and then every `store_probe_interval_s` (300 to 900,
refused outside the range at load) once a device declares
`gateway-evidence-carriage/v1`, and every 900 seconds on a site that declares
nothing, which does not consume the key: the probe reads the directory
and atomically replaces the fixed private file `.ori-evidence-probe`, which the
loader ignores; it never touches a queue record. `last_error` stays a one-event
summary.
Every stall that is not a hold is logged once on entry and again on the
reminder interval.

Verified by the tests in `internal/evidence/courier`, including
`TestTerminalRefusalIsAttemptedOnceHoldsTheQueueAndIsLoggedOnce`,
`TestReceiverStateRefusalRetriesOnNextHandoffAndNeverBlocks`,
`TestHoldOnOneDeviceNeverDelaysAnother`,
`TestBackoffOnOneDeviceNeverDelaysAnother`,
`TestHandoffForOneDeviceNeverRetriesAnother`,
`TestPerDeviceCapacityRefusesOnlyTheExhaustedDevice`,
`TestAdmissionRefusesAnArtifactOverTheTransportMaximum`,
`TestGatewayWideQueueOpensPerDevice`, `TestBackoffSurvivesARestart`,
`TestRestartRestoresHoldWithoutSendingIt`,
`TestInvalidHoldRefusesTheQueueAndKeepsTheRecord`,
`TestEachProcessWithoutADurableHoldSendsOnceUntilOneIsWritten`,
`TestEveryNonAcceptingOutcomeIsLogged`, the
`evidence-transport/vectors/refusal-policy-v2.json` corpus through
`TestRefusalPolicyVectorCases` and `TestRefusalPolicyVectorSequences`, and in
`cmd/ori-gateway` by `TestRealProjectionsSatisfyTheContract` and
`TestProjectionOracleAgreesWithTheVectors`.

20. `GW-20` Contract vectors are vendored, and an unmet case is pinned, never skipped.
Every `ori-specs` vector a test reads is vendored in `internal/specvectors`,
pinned to one `ori-specs` commit by its `MANIFEST.json` and read through
`specvectors.Read`; no code, script, build file or workflow reads an `ori-specs`
checkout. A contract case this gateway does not yet satisfy runs as an expected
failure, satisfied only by its exact observed deviation, and names a stable
`missing_requirement:<id>` and the issue that owns it; a pinned case that now
conforms fails until its pin is removed. No issue or pull request number
appears in production code, comments, test prose, documentation, error
messages or any string, with one exception: the structured `Issue` field of a
pin, a full `https://github.com/ori-platform/ori-gateway/issues/<n>` URL,
because a new pin must not acquire a requirement without an owned disposition.
Verified offline by `TestEveryVendoredFileMatchesTheManifest`,
`TestNoTestReadsASiblingCheckout` and `TestPinsLinkEachRequirementToOneIssue`:
one issue per requirement, each issue serving only the requirements its
registry entry lists, and no issue URL anywhere but that field.

## Layout

```text
cmd/ori-gateway/
internal/broker/
internal/config/
internal/contracts/
internal/dispatcher/
internal/evidence/
internal/fleet/
internal/heartbeat/
internal/provider/
internal/reporting/
internal/runtimeclient/
internal/session/
internal/sim/
internal/site/
internal/webhookbridge/
```

## Verification

```bash
go test ./...
gofmt -w .
```

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

9. `CGO_ENABLED=0` must be set in CI. The gateway has no CGO dependency.

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
