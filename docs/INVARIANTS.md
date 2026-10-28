# Gateway invariants: full text

This file holds the full, binding text of every gateway invariant, with the tests that verify each one. [`AGENTS.md`](../AGENTS.md) carries a short summary of each and points here; where the two differ, this text governs. Change an invariant here and its summary in `AGENTS.md` together.

## GW-1: Topic names must match [`ori-specs/gateway-api/v2.md`](https://github.com/ori-platform/ori-specs/blob/main/gateway-api/v2.md) exactly

## GW-2: Every response must echo the request `request_id`

A missing or changed `request_id` causes [`ori-runtime`](https://github.com/ori-platform/ori-runtime) timeout and fallback behavior.
Provider timeout, provider error, provider panic, invalid provider response, and
publish-failure paths must still preserve the original `request_id` whenever the
incoming request is valid enough to correlate. Verified by:
`TestDispatcherTimeoutErrorResponse`, `TestDispatcherProviderErrorResponse`,
`TestDispatcherProviderPanicResponse`,
`TestDispatcherProviderInvalidResponsePublishesErrorResponse`,
`TestDispatcherPublishFailureIsSurfaced`, and
`TestAResponseThatCannotBeEncodedIsAnsweredWithAnError`.

## GW-3: Providers must satisfy one shared interface

Provider-specific settings belong in config, not in the provider interface.
Tier 3 reasoning providers are selected through the reasoning provider factory
only. Customer-reporting providers must not be routed through that factory.
Verified by `TestReportingProviderDoesNotAffectReasoningProvider`.

## GW-4: Gateway never changes action authority

[`ori-runtime`](https://github.com/ori-platform/ori-runtime) owns action-tier authority. Gateway may echo tier hints but must not
promote or downgrade physical authority.

## GW-5: Gateway is not in the Tier D path

Tier D safety fires locally in the [runtime](https://github.com/ori-platform/ori-runtime) rule engine before gateway, cloud, or
network systems are consulted.
No direct test currently enforces this; it is enforced by code review and by
keeping Tier D evaluation and actuation APIs out of this repository.

## GW-6: Heartbeat must be reliable

If heartbeat publication cannot continue, the gateway must report failure or
exit rather than silently appearing available.
The gateway process must also surface repeated reasoning-response delivery
failures instead of logging forever while dropping all replies. Only a reply
it could not publish counts toward that limit (`dispatcher.ErrPublishResponse`):
a request it refuses, malformed, unauthenticated, not fresh or with a
`request_id` already in flight for the same device
(`dispatcher.ErrRequestInFlight`), neither
counts nor resets the count, so no sender can end the gateway by sending junk
or mask a delivery failure by repeating a request. A
heartbeat that the run's own stop interrupts, or that the signing clock the
stop has closed refuses, is not a failure, and neither is a reasoning reply
the stop refuses, by cancelling it or closing the signing clock, so a clean
stop is never reported as a failure.
Verified by `TestHeartbeatFailureLimit`,
`TestGatewayHeartbeatFailureReturnsError`,
`TestMainEscalatesRepeatedRequestHandlerFailures`,
`TestMalformedReasoningRequestsNeverStopTheGateway`,
`TestADuplicateInFlightRequestNeitherCountsNorResetsTheFailureCount`,
`TestARequestAlreadyInFlightIsRefusedAndDeliversNothing`,
`TestTheSameRequestIDFromTwoDevicesIsAnsweredForEach`,
`TestTheSameRequestIDFromTwoDevicesIsAnsweredThroughTheGateway`,
`TestAPublishTheStopInterruptsIsNotAFailure`,
`TestAHeartbeatTheStoppedClockRefusesIsNotAFailure`,
`TestAReplyTheStopRefusesIsNotADeliveryFailure`,
`TestAReplyFailingAfterTheCallerCancelsIsNotCounted`, and
`TestARefusalByAClosedClockWhileTheRunIsLiveIsNotCounted`.
Neither escalation waits on the log destination. `runGateway` queues the
gateway's logger through `internal/asynclog`: records are written off every
caller's path, in order, by one drainer, with at most 1,024 waiting behind a
logger that blocks and later ones counted. Every component it hands that
logger to shares the one queue, the heartbeat publisher and the broker client
among them. Each of those also queues any other logger it is given, and a
replay verifier given none queues `slog.Default()`, so nothing on a callback
or an escalation logs synchronously. A heartbeat whose every log line would
block still reaches its failure limit and exits, after giving its last line a
bounded chance (`asynclog.ExitFlushBound`, 1 s) to reach the destination. The
request escalation still ends the run, and `runCLI` writes its final line
within the same bound and exits whether or not it was taken. Site
health reports the gateway `degraded` while its heartbeat is not going out:
before the first one, after a failed attempt, or with none for three
intervals. Verified by `TestALoggerThatBlocksNeverKeepsTheHeartbeatFromItsExit`,
`TestTheExitLineIsWrittenBeforeTheHeartbeatExits`,
`TestPublishingFollowsTheLatestAttempt`,
`TestTheRequestEscalationEndsTheGatewayBehindALoggerThatBlocks`,
`TestTheRequestEscalationEndsTheProcessBehindABlockedLoggerAndStderr`,
`TestABlockedDefaultLoggerNeverHoldsAVerifierOrAStop`,
`TestRunCLIExitsBehindAStderrThatBlocks`,
`TestSiteHealthIsDegradedOnceHeartbeatsStop`,
`TestALoggerThatBlocksNeverHoldsTheClientsDelivery` and the
`internal/asynclog` tests.

## GW-7: Fleet/cloud forwarding is opt-in

With fleet disabled, gateway must not call `ori-cloud`, resolve cloud hosts, or
attempt authentication.
Disabled fleet must perform zero HTTP, DNS, auth, credential, or background
cloud work. Verified by `TestDisabledFleetNoNetwork` and
`TestMainDisabledOptionalModulesDoNotFailStartup`.

## GW-8: Site coordination is LAN-scoped

Cross-device correlation and shared GSM are site functions. Fleet analytics and
billing belong in `ori-cloud`.

## GW-9: Gateway must not persist reasoning results

Stateful learning and causal memory belong in [runtime](https://github.com/ori-platform/ori-runtime) or cloud-defined stores, not
in the gateway request proxy.
No direct test currently enforces this; it is enforced by code review and by
keeping durable reasoning stores out of the gateway request path.

## GW-10: Product reporting providers are separate from Tier 3 reasoning providers

The gateway may own connected customer-reporting and enrichment providers, but
those providers must use reporting-specific config and must not be wired into
the Tier 3 reasoning provider factory.
Verified by `TestReportingProviderDoesNotAffectReasoningProvider`.

## GW-11: Gateway reporting and enrichment never change action authority

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

## GW-12: Reporting provider credentials stay out of runtime config

Gemini/API keys and equivalent product-provider credentials belong in gateway
or product environment variables only. Secret values must never be committed,
and runtime config examples must remain provider-neutral.
Only environment variable names may appear in gateway config. Verified by
`TestReportingAPIKeyEnvMustBeEnvVarName`.

## GW-13: SIM access is opt-in

With SIM disabled, gateway must not initialise, open, probe, or otherwise touch
modem or serial hardware. Verified by `TestDisabledSIMNoSerialProbe` and
`TestMainDisabledOptionalModulesDoNotFailStartup`.

## GW-14: Gateway data access goes through runtime-owned interfaces

Gateway must not read runtime SQLite files directly. Sensor history, action
logs, Tier C decisions, and runtime health must be consumed through explicit
runtime-owned export interfaces. Runtime health posture fields such as broker
hardening, state-store encryption, and alert outbox backlog must be mapped from
the health export rather than inferred by reading runtime files. Current
enforcement is structural: the gateway has no SQLite driver dependency and
runtime-client tests such as `TestTierCDecisionLogRequestValidation` validate
the export request contract. Future transport tests must preserve this boundary
and must not add direct SQLite access.
The severity of a runtime health export is read once, in
`runtimeclient.MQTTClient.Health` after the export has verified, by
gateway-api/v2's reading of `status` while runtimes adopt runtime-health/v4,
and every consumer, site health and the weekly report among them, reads it
from `HealthSnapshot.Status`: `ok`, `degraded`, `critical` or `unknown`.
Critical is never read as less; `ok` comes only from an explicit `ok` in a
marked projection or from an unmarked item well formed for the after-v2.5.0 or
the v2.5.0 encoding that states no condition holds, so an absent, unknown,
mistyped or inconsistent member reads `unknown`, logged once with a fixed fault
name and never the export's content. Once a device has served the projection,
its unmarked items read `unknown`, unless critical, for as long as the client
runs; the device is the requested one, never the item's `device_id`.
Verified by the `gateway-api/vectors/health-transition-v2.json` corpus and its
baselines through `TestHealthTransitionCorpus` (the verified export path, with
encryption off and on) and
`TestHealthTransitionReachesSiteHealthAndTheWeeklyReport` (into the site
posture, the site health projection and the weekly report input), and by
`TestALatchBelongsToTheRequestedDevice`, `TestAFaultIsLoggedOncePerEpisode`,
`TestConcurrentReadingsShareOneReader` (under `-race`) and
`TestSiteExportSeverityNeverReadsAnUnknownValueAsOK`.

## GW-15: Gateway process startup and shutdown must be ordered and explicit

Startup order is config, the gateway state directory (when `gateway.auth` is
enabled), the evidence stores (when evidence is enabled), provider, broker
connect, optional modules, heartbeat,
dispatcher, then MQTT subscribe. Failure at any required stage must prevent
later stages from starting. Shutdown must cancel background work and disconnect
the broker. Long-running app tasks must be supervised through the shared
`errgroup`-backed supervisor rather than open-coded result channels, so adding a
new runner does not require updating every early-return drain path by hand.
Every broker callback, on both broker clients, runs through one callback gate,
and reasoning and enrichment requests and evidence carriages and
acknowledgements, which run off the callback, and posture fetches started from
the runtime heartbeat callback are owned the same way: once shutdown begins
none starts, and `runGateway` does not return while one is still running. A
message accepted off the callback but still waiting when the stop is decided
is released without being handled; its PUBACK was already sent, so the broker
does not deliver it again, and the runtime's own retry or fallback covers it. The decision to stop is one step
(gateway-config/v2 `gateway.state_directory` rule 13): it refuses every later
broker callback, on both broker clients, every later webhook request, which
the bridge answers 503 without reading it, and every later posture fetch,
stops the signing clock, which from then on refuses to sign and starts no
write of its mark, so a callback still running sends no reply, and cancels
the run. It is taken where the stop happens: when the supervised runners' context ends, on the
parent's cancellation or a runner's failure, and in the callback that reaches
the request failure limit, as well as on every other stop path; it always
comes before any callback is joined or the state directory released. Every
supervised runner, the webhook bridge, site health server and site node
eviction among them, is cancelled by the decision itself, so none runs on
while runGateway is still in a startup step, and a startup step that then
fails, or a caller that cancels before it is noticed, reports the request
failure escalation, not its own cancellation;
callbacks, webhook requests and fetches already admitted are joined after
that. The state directory is released only once no write of the mark is
outstanding; when one still is at the quiescence bound, 5 seconds measured once
from the decision to stop, the process ends with status 1 without releasing it,
and no further run begins in that process. A start refused while its first
write is outstanding ends the same way. Quiescence is awaited for the whole
bound, so a write completing just before it releases the directory; a warning
is logged shortly before the bound, and nothing on the exit path waits on the
logger beyond a stated bound, so a logger that blocks cannot keep the process
alive. The state-directory exit waits on it not at all. A run that ends gives
its queued log records up to `asynclog.ExitFlushBound` (1 s) to be written, and
`runCLI` gives its final line the same bound (GW-6).
Verified by `TestMainStartupMissingConfig`,
`TestGatewayStartupProviderFailureStopsBeforeBroker`,
`TestGatewayStartupConnectFailureStopsBeforeOptionalModules`,
`TestGatewayStartupSubscribeFailureCancelsHeartbeatAndDisconnects`,
`TestMainStartsHeartbeatBeforeSubscribe`, `TestGracefulShutdown`,
`TestSupervisedRunnersReportsRunnerErrors`,
`TestSupervisedRunnersWaitsForAllRunners`,
`TestSupervisedRunnersUnexpectedNilReturnIsError`,
`TestShutdownWaitsForAPostureFetchesSigningWrite`,
`TestShutdownJoinsEveryBrokerCallback`,
`TestShutdownJoinsARequestRunningOffTheCallback`,
`TestNoRequestStartsOnceTheStopIsDecided`,
`TestAMessageWaitingWhenTheRunStopsIsReleasedUnhandled`,
`TestAClosedPostureDebouncerStartsNothing`,
`TestShutdownWaitsForTheGatewaysOwnExtension`, `TestAClosedClockRefusesToSign`,
`TestAStoppingGatewayWithAStalledWriteEndsTheProcessHoldingTheLock`,
`TestAStoppingGatewayThatReachesQuiescenceReleasesTheLock`,
`TestAStartWhoseFirstWriteStallsIsRefusedHoldingTheLock`,
`TestAStalledWriteHoldsTheLockUntilTheProcessEnds` (a child process),
`TestABlockedLoggerNeverDelaysTheRequiredExit`,
`TestAWriteCompletingJustBeforeTheBoundReleasesTheLock`,
`TestAStateDirectoryRefusalCreatesNoEvidenceQueue`,
`TestNoCallbackIsAdmittedAfterTheDecisionToStop`,
`TestARunnerFailureDecidesTheStopBeforeTheRunnersAreJoined`,
`TestTheDecisionToStopRefusesEveryAdmissionInOneStep`,
`TestARefusedGateStillJoinsWhatItAdmitted`,
`TestTheFailureLimitStopsEveryAdmissionBeforeTheGatewayNoticesIt`,
`TestTheFailureLimitIsReportedWhenStartupCompletes`,
`TestTheFailureLimitIsReportedWhenTheCallerCancelsLater`,
`TestShutdownJoinsAnAdmittedWebhookRequest` and
`TestARequestTheGatewayNoLongerAdmitsIsNeverReadOrForwarded`.

## GW-16: Public SMS provider ingress must be signed before it reaches runtime

Providers such as Africa's Talking do not emit Ori HMAC headers. The gateway
webhook bridge is the allowed production adapter for that case: it validates
source CIDRs, caps request body size, preserves the raw provider body, adds
`X-Ori-Webhook-*` HMAC headers using environment-sourced secrets, and forwards
only to the runtime's local webhook URL. Gateway heartbeat may expose bridge
readiness and limit posture, but `ready` must reflect bridge-loop liveness rather
than static config. It must never expose target URLs, env var names, provider
CIDR values, SMS body content, phone numbers, bearer tokens, or HMAC secrets.
After the source check and before signing, the bridge bounds volume: a token
bucket per sender, keyed by HMAC-SHA256, under a key that never leaves the
process, of the sender read the runtime's way where both readings agree
(`from`, else `from_number`, from a form or JSON body, digits and `+` only;
any other shape naming a sender field shares one reserved, limited key), and a
bridge-wide ceiling. A message naming no sender, such as a delivery report,
draws on the ceiling alone, and a sender the ceiling refuses keeps its token. A
refused request is answered 429 and never forwarded. Sender state is capped
(`max_senders`), evicting the least recently seen. The per-sender limit is
refused at load if tighter than the runtime's own on approval replies (10 a
minute, burst 5), and the ceiling if below it; the bridge never decides
authority or filters by an approver list. Refusals are counted for site health (`webhook_bridge_refused`)
and logged paced with the limit name only. Once the gateway has decided to
stop, the bridge answers every request 503 without reading it, and releases
an admitted request only once it is answered (GW-15).
Verified by
`TestOneSendersBurstIsRefusedWhileAnotherSenderIsForwarded`,
`TestEverySpellingOfOneNumberIsOneSender`,
`TestSendersTheBridgeCannotReadShareOneLimitedBucket`,
`TestDeliveryReportsDrawOnTheCeilingAlone`,
`TestAFloodNeverSpendsAnOperatorsOwnAllowance`,
`TestEveryRefusalIsLoggedEventually`,
`TestSiteHealthCountsTheBridgesRefusals`,
`TestDistinctSendersAreHeldByTheGlobalCeilingInBoundedMemory`,
`TestForwardedBodiesStayExactAndSignedUnderLimiting`,
`TestRefusalsLogNoSenderHashOrContent`,
`TestWebhookBridgeRejectsInvalidConfig` and
`TestARequestTheGatewayNoLongerAdmitsIsNeverReadOrForwarded`.

## GW-17: Runtime export clients must match runtime MQTT security posture

When `gateway.auth.enabled=true`, export requests must be HMAC-signed and
export responses must be verified with current-or-previous gateway secrets.
When `gateway.encryption.enabled=true`, sensitive runtime export responses
(`sensor_history`, `action_log`, `reasoning_log`, `tier_c_decision_log`) must
be AES-GCM decrypted after HMAC verification. Health exports may remain
plaintext but still authenticated. New outbound signatures must use the current
secret only; previous secrets are verify-only.

## GW-18: Site health projection is read-only, advisory, and secrets-free

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
Severity is never folded: a node's own `critical` stays `critical` in the node
view and the site status, staleness and evidence-integrity problems only raise a
node's status, and the site status is the most severe of the gateway and every
node, `critical` over `degraded` over `healthy`. A node whose last heartbeat
was `critical` is never evicted for silence: it stays `critical` until it
reports again or is removed from `gateway.device_ids` (`docs/OPERATIONS.md`).
The severity read from a node's health export (GW-14) only raises its status,
critical to critical and anything but ok to degraded, and is not itself a
projected member. The registry keeps it through every heartbeat, a failed
fetch and eviction, and records a reading that arrives while the node is not
registered; only a later export reading changes it, and a node whose
export last read critical is retained on silence like a critical heartbeat.
The TTL for silence, eviction and staleness is one fixed value,
`site.NodeTTL`, 90 seconds (gateway-api/v2 Site Health), never derived from
`heartbeat_interval_s` and not configurable, so no TTL outside 90 seconds to
24 hours can be set. Verified by
`TestSiteHealthJudgesNodesOnTheNinetySecondTTLWhateverTheHeartbeatInterval`,
`TestEvictionAndTheProjectorShareTheNodeTTL` and
`TestTheNodeTTLIsTheContractsNinetySeconds`.
Whether a node is still reporting is decided by the gateway's receipt of its
heartbeats alone, measured on a monotonic clock the registry owns: silence,
eviction and `gateway_observed`. A node's `stale` flag is set when its report
has not moved for the TTL on the same clock: no heartbeat arrived, or none
whose `last_seen_ms` exceeded the previous heartbeat's (a live runtime's
advances on every heartbeat). No wall clock is read, so neither an offset node
clock nor a step of the gateway's own wall clock makes a node stale or fresh.
`last_seen_ms` is shown as reported. `gateway_seen_ms` is the wall-clock
receipt, for display only. A producer-supplied `gateway_seen_ms` is admitted
and never read.
Verified by `TestAReportingNodeIsNeverEvictedWhateverItsClockSays`,
`TestASilentNodeIsEvictedAtTheTTLFromItsLastReceipt`,
`TestAWallClockStepNeitherSilencesNorHidesANode`,
`TestProjectSiteHealthAnOffsetNodeClockIsNotStale`,
`TestProjectSiteHealthAStuckReportGoesStaleWhileReceived`,
`TestProjectSiteHealthASteppedBackNodeClockAdvancesAgain`,
`TestProjectSiteHealthAWallClockStepNeitherStalesNorFreshens`,
`TestAReportingNodeWithASkewedClockStaysObservedThroughGetHealth`,
`TestAWallClockStepNeitherSilencesNorHidesANodeThroughGetHealth` and
`TestRuntimeHeartbeatHandlerUpdatesRegistry`.
An expected node the registry does not hold is degraded, and critical when
its recorded export severity is critical.
Verified by `TestCriticalHeartbeatKeptThroughGetHealth`,
`TestExportSeverityOnlyRaisesANode`,
`TestExportSeverityOutlivesEveryHeartbeat`,
`TestASilentNodeWhoseExportReadCriticalStaysCritical`,
`TestACriticalExportSurvivesHeartbeatsAndAFailedFetch`,
`TestALateExportReadingAfterEvictionIsKept`,
`TestAnUnobservedNodeKeepsItsRecordedExportSeverity`,
`TestProjectionReadsSeverityUnderTheLock`,
`TestEvictStaleRuntimeNodesReportsASilentCriticalExport`,
`TestARunningGatewayKeepsACriticalExportThroughAFailedFetch`,
`TestCriticalNodeNeverFoldedBelowCritical`,
`TestFallbackAfterEvidenceHistoryStaysCritical`,
`TestSilentCriticalNodeStaysCriticalUntilItReportsAgain`, and
`TestEvictStaleRuntimeNodesRetainsSilentCriticalNode`.
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
`TestProjectSiteHealthAnOffsetNodeClockIsNotStale`,
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

## GW-19: The gateway is a blind, durable evidence courier

Outbound evidence artifacts must be queued byte-for-byte before custody is
acknowledged, and queue exhaustion must be an explicit retriable refusal.
Custody is authenticated under the dedicated `gateway_custody` secret and must
never be treated as an authority receipt. Delivery envelopes, registrations,
commissioning authorisations, and checkpoints remain end-to-end signed; the
gateway does not verify, rewrite, or re-sign them. Authority receipts and epoch
confirmations remain opaque and must be durably staged before outbound queue
retirement. The gateway carries them to the runtime re-encoded as canonical
JSON, which changes no value (gateway-api/v2 Artifact Bytes), and an inbound
acknowledgement names one by the SHA-256 of those canonical bytes. The evidence channel shares no authority, keys, storage,
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
(a handoff for both its lanes), signalled once its bytes are durably queued
whatever later step fails, an authenticated admission for the same device
refused `queue_full` (a handoff for both its lanes that admits no bytes and
releases no authority-artifact hold), a `200` retiring an anchor registration for
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
`gateway.device_ids` entry that declares `gateway-evidence-carriage/v1` must
be in the evidence routing domain, which refuses a control character (Cc, NUL
among them); any other entry is held only to the `gateway.device_ids` rule
(gateway-config/v2). A carriage whose topic names an unconfigured
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

An authority artifact returned to a runtime retires only on an authenticated
acknowledgement from the device the artifact is for, naming it: `applied`, or a
refusal the `gateway-evidence-carriage/v1` retirement table marks retiring. The
site shares one envelope secret, so another device's correctly signed decision
on its own topic is refused by the queued-device binding, not by the envelope.
A refusal that does not retire, including one whose reason is outside the
`evidence-exchange/v3` receiver rejection vocabulary, holds the queue entry it
was decided on, and only that entry; it is carried again only on a receiver
state change for the device it is for: an artifact from that device newly
admitted, never a carriage of bytes already queued, another device's handoff,
a carriage the ingress refuses, or a queue wake. An unrecognised reason is logged quoted and bounded. Verified by
the `gateway-evidence-carriage/vectors/inbound-retirement-v1.json` corpus
through `TestInboundRetirementCorpus`, and by `TestUnknownInboundReasonIsHeld`,
`TestAHoldBindsOnlyTheHeadItWasDecidedOn`,
`TestAMisboundRetiringRefusalRetiresNothing`,
`TestInboundAckResultLogsAnUnrecognisedReasonQuoted`,
`TestAnotherDevicesHandoffNeverReleasesAHold`,
`TestAQueueWakeNeverReleasesAHold`, `TestARefusedAdmissionReleasesNoHold`,
`TestACarriageOfAnArtifactAlreadyQueuedIsNotAHandoff`,
`TestAHandoffSurvivesAFailureAfterDurableAdmission`,
`TestAStaleDecisionNeverReplacesAnotherHeadsHold`,
`TestARetiredHeldEntryStagedAgainIsCarried` and
`TestAnIllTypedAcknowledgementIsNoDecision`, and in `cmd/ori-gateway` by
`TestRunningGatewayReleasesAHoldOnlyOnItsDevicesHandoff`. An acknowledgement member that is
absent, null or of another JSON type is no decision: a null or absent `reason`
never reads as `applied`'s empty one.

## GW-20: Contract vectors are vendored, and an unmet case is pinned, never skipped

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

## GW-21: MQTT ingress is bounded before it is read

Every subscription on either broker client belongs to a gateway-api/v2 Payload
Size family with a maximum and an ingress rate, or `broker.Client.Subscribe`
refuses it. The client's message wrapper compares a payload's length with the
family maximum, then applies the sending device's message and byte rate (a wildcard
subscription keeps one per configured device and one shared by every other
device segment), before the handler runs: a refused payload is never parsed,
decoded, verified, routed or answered, and changes no state. Refusals are
counted per family, projected as site health `gateway.mqtt_ingress`, and
logged paced, with family and size class only, never content, device ID or
topic. No setting raises or lowers a maximum or a rate. Reasoning and
enrichment requests, outbound evidence carriages and inbound evidence
acknowledgements are handled off the client's ordered delivery, so neither a
provider call nor a reply or acknowledgement waiting for its PUBACK holds any
other message. A device's carriages and acknowledgements share one queue and
are handled one at a time, in the order they arrived, as on the client. What
each device holds of a family is bounded as the rate is, at the family's
burst, and a message over the bound is dropped unanswered, counted as a volume
drop and logged apart from the rate. A pool refuses to subscribe a topic
outside the families it serves.
Every required reply
the gateway produces (reasoning and enrichment error responses, the outbound
evidence acknowledgement, the gateway heartbeat) fits the 4096-byte floor at
the contract's bounds: an error response, whoever built it, carries an `error`
of at most 128 scalars and no other free-form content, and a reasoning or
enrichment request whose `request_id`, or enrichment `proposal_id`, is outside
`^[A-Za-z0-9._-]{1,128}$` is refused unanswered. A reasoning or enrichment
response over its maximum is replaced by the error `response_too_large`. An
inbound evidence acknowledgement that decides nothing the gateway holds, and
an export response whose request is no longer pending, add no replay state. Inbound evidence is published as canonical JSON, and staging
admits no authority artifact over 1,048,576 canonical bytes; the evidence
channel's 1 MiB response cap already refuses one first, so the staging bound
is defence in depth.
Verified by `TestEverySubscriptionHasAPayloadMaximum`,
`TestAPayloadOverItsTopicMaximumIsRefusedBeforeParsing`,
`TestPayloadSizeVectorCases`,
`TestARefusedOversizePayloadChangesNothingAndIsNotAnswered`,
`TestAnOversizePayloadCostsNoParse`, `TestAVolumeOverTheRateIsDroppedAndCounted`,
`TestABurstOfReasoningRequestsIsAnsweredWithoutStalling`,
`TestAHeartbeatIsNotDelayedByAReasoningCall`,
`TestEachDeviceHasItsOwnAllowanceOfRequestsInFlight`,
`TestABurstOfOutboundCarriagesIsAcknowledgedWithoutStalling`,
`TestAnOrderedPoolKeepsEachDevicesArrivalOrder`,
`TestAnOrderedPoolBoundsWhatADeviceHolds`,
`TestABurstOfEnrichmentRequestsIsAnsweredWithoutStalling`,
`TestCarriagesAndAcknowledgementsShareEachDevicesOrder`,
`TestTheRunningGatewayKeepsEachDevicesEvidenceOrder`,
`TestAPoolRefusesATopicOutsideItsFamilies`,
`TestADropAtTheHeldBoundIsLoggedApartFromTheRate`,
`TestAPanickingRequestIsRecoveredAndReleased`,
`TestIngressRefusalsAreRecordedWithoutContent`,
`TestAWildcardSubscriptionRatesEachDevice`,
`TestAByteBudgetBoundsTheParseWorkEachDeviceCanCause`,
`TestRequiredRepliesFitTheFloor`,
`TestInboundEvidenceIsPublishedCanonical`,
`TestAProviderReturnedErrorIsSentAsTheBoundedErrorResponse`,
`TestAnAcknowledgementThatDecidesNothingAddsNoReplayState`,
`TestACancelledExportResponseAddsNoReplayState`,
`TestAnOversizeAuthorityArtifactHoldsOnlyItsLane` and
`TestProjectSiteHealthMQTTIngressIsExactlyTheReceivedFamilies`.

## GW-22: A runtime-facing payload has one reading

Every payload the gateway receives from a runtime is admitted under
gateway-api/v2's Payload Member Rule in the broker's message wrapper, after
size and rate and before any handler runs, through `contracts.AdmitPayload`
and ori-canonicaljson's strict decode: invalid Unicode, malformed JSON,
nesting past 32, trailing data, a duplicate member, a name equal to another or
to a defined member under the closed fold, and a value that is not an object
are refused, unanswered, and logged with the reason only. Each family's defined
members are `contracts.DefinedMembers`, held equal to the vendored corpus. A
decrypted export plaintext is admitted the same way before it is read; one
the rule refuses is logged with its reason and changes nothing: no replay key
is recorded and the export request stays pending. An
authority artifact that routes correctly is judged at staging inside the
inbound message that would carry it; one the rule refuses is not staged and,
as for an artifact over the inbound maximum, the attempt is an unrecognised
outcome (evidence-transport/v2): the entry is retained and backs off. No
inbound message the rule refuses is ever published. Every struct a runtime payload is decoded
into, or encoded from, names only its family's defined members, so a struct
decode of an admitted payload reads members by exact name. A member no section
defines is still tolerated where its section tolerates unknown members.
Verified by `TestDefinedMembersMatchTheCorpus`,
`TestPayloadMemberVectorCases`,
`TestAPayloadTheMemberRuleRefusesChangesNothing`,
`TestRuntimeFacingStructsNameOnlyDefinedMembers`,
`TestEvidencePayloadStructsNameOnlyDefinedMembers`,
`TestExportPayloadStructsNameOnlyDefinedMembers`,
`TestEveryExportRequestTheClientPublishesIsAdmitted`,
`TestDecryptedPlaintextIsHeldToTheMemberRule`,
`TestARefusedExportPlaintextChangesNothing`,
`TestVerifierFoldsNamesOnlyByTheClosedFold`,
`TestAnAuthorityArtifactTheMemberRuleRefusesIsUnrecognised`,
`TestRoutingIsJudgedBeforeTheMemberRule`,
`TestAnInboundMessageTheMemberRuleRefusesIsNeverPublished` and
`TestARefusedCoveringReceiptLeavesTheAcceptanceUnrecognised`.

## GW-23: An export exchange ends only on a verified response or its bound

With `gateway.auth.enabled`, `runtimeclient.MQTTClient` verifies an export
response under the device_id and request_id of the request waiting for it,
correlated by the response topic, before delivering anything. A response that
is not verified is dropped and logged, records no replay key, and leaves the
request waiting, so no broker client can end an exchange by publishing on its
response topic. Every export request's wait is bounded inside the client, in
both auth modes: from publishing the request, the publish included, with
subscribing bounded the same way, and no message received extends it. On
expiry the request fails with `ErrExportTimeout`, is logged, and releases its
pending state; a later attempt is a new, newly signed request. A response is
handed over only while its request's wait has not expired, decided in the step
that hands it over, so one arriving after the deadline completes nothing and
records no replay key. One subscribe per response topic is in flight at a
time, and every export waits on it within its own remaining bound; no lock or
transport window extends that, and the shared subscribe ends with the run.
Requests are paced per device below the rate the runtime admits export
requests (the runtime admits 2 a second, bursts of 10, ori-runtime
`payload_bounds.py`, since gateway-api/v2 Payload Volume leaves the value to
the runtime; the gateway sends at most 1.6 a second, bursts of 8, each device
starting empty), because the runtime judges arrivals, not sends; pacing is
bounded like the wait. A response handed over in time completes its request
whichever the wait sees first, and even when the publish that carried the
request then reports failure; the request's pending state is released before
its response is last read, so none is handed over and then discarded. A
broker `Connect`, `Subscribe` or `Publish` waits for its acknowledgement only
while its caller's context lasts, and 30 seconds at most without a deadline,
so ending the run ends a shared subscribe at the transport. The export
client's log records, its verifier's included, are written off every caller's
path, in order, by one drainer, which also asks the logger's level, at most 64
waiting behind a logger that blocks and later ones counted (in the gateway the
logger is its shared queue, GW-6, which never blocks, so the records wait
there among the 1,024 and a drop is counted there), so none of them holds an export past its bound, delays
the release of its pending state, or holds the broker callback that delivers
responses. An export page missing any
envelope member (`request_id`, `export_type`, `device_id`, `items`,
`next_page_token`, `complete`, `error`) fails the export as malformed and is
never read as complete, empty or successful. The page count of one export is
bounded, so no caller waits on an export without a bound (gateway-api/v2
Export Auth Envelope). Verified by
`TestAForgedExportResponseLeavesTheExchangePending`,
`TestASilentRuntimeEndsTheExportAtItsBound`,
`TestAStalledSubscribeOrPublishEndsAtTheBound`,
`TestRepeatedForgeriesNeverExtendTheBound`,
`TestAResponseOnAnotherDevicesTopicNeverAnswers`,
`TestAnExportRequestsABoundedNumberOfPages`,
`TestAResponseAfterTheDeadlineNeverCompletesTheExport`,
`TestAResponseIsRefusedOnceTheDeadlineHasPassedBeforeItsTimerFires`,
`TestConcurrentSubscribesEndWithinTheBound`,
`TestASharedSubscribeThatExpiresEndsEveryWaiterAsATimeout`,
`TestTheEnforcedEnvelopeIsTheContractsEnvelope`,
`TestAnExportPageMissingAnEnvelopeMemberFailsTheExport`,
`TestExportsArePacedPerDeviceAtTheRuntimesRate`,
`TestAPacingWaitPastTheBoundEndsTheExport`,
`TestPacedExportsSurviveArrivalJitterAtTheRuntime`,
`TestProductionPacingWaitsOnTheRealClock`,
`TestPacingNeverSpinsOnAnInexactRate`,
`TestAResponseHandedOverInTimeCompletesTheExport`,
`TestEndingTheLifetimeAbandonsAStalledSubscribe`,
`TestTheRuntimeExportClientEndsWithTheRun`,
`TestAResponseHandedOverInTimeSurvivesAFailedPublish`,
`TestAResponseHandedOverAsItsRequestIsReleasedIsNeverDiscarded`,
`TestATimeoutNeverWaitsOnTheLogger`,
`TestAResponseRefusedBelowTheFloorNeverHoldsTheCallback`,
`TestClientLogLinesKeepTheirOrderAndAreBoundedBehindABlockedLogger`,
`TestALoggerThatLocksItsLevelCheckNeverHoldsAnExport`,
`TestALoggerThatEnablesOnlyErrorsIsWrittenNothing`,
`TestASubscribeEndsWhenItsContextIsCancelled`,
`TestWaitTokenEndsWhenItsContextIsCancelled`,
`TestWaitTokenTakesATokenThatCompletedAsItsContextEnded`,
`TestWaitTokenWithoutADeadlineIsBounded` and
`TestAWeeklyReportForManySensorsStaysWithinTheRuntimeExportRate`.

## GW-24: Envelope freshness and replay follow gateway-api/v2

Every runtime-gateway HMAC envelope the gateway verifies is checked in the
contract's order, stale, future, below the replay floor, replay, each refused
under its own name (`mqttauth.ErrStaleTimestamp`, `ErrFutureTimestamp`,
`ErrBeforeReplayFloor`, `ErrReplayDetected`). A key is kept until its envelope
is stale; `replay_ttl_ms` is only a lower bound. Every dropped or evicted key
raises the floor past it in the same step, and a full verifier evicts its key
with the smallest signing time. The gateway stores neither its floor nor its
keys, so each verifier starts its floor at its clock: an envelope signed before
the start is refused, and the contract's stated limits for a receiver that
stores nothing apply. A family that records only once an envelope answers
something rechecks the floor and replay in one step with recording, and a
recorded key is never un-recorded. A refusal below a floor ahead of the stale
bound is logged with the floor and the clock, once the replay lock is
released, so a logger that blocks holds no other verification. Skew is fixed
at 5 minutes.
Every envelope the gateway signs takes `signed_at_ms` from a signing clock: the
evidence families from their own durable clocks, and every envelope signed
under the runtime–gateway HMAC outside the evidence path (today the heartbeat,
reasoning and enrichment responses and export requests) from one shared
`mqttauth.SigningClock`, the message clock. A signer added later on that path,
such as a LAN one, uses it too. Within a process no clock repeats a value. A
gateway that signs keeps the message clock's mark in `gateway.state_directory`
under gateway-config/v2, which is required when `gateway.auth.enabled` and is
owned by `internal/gatewaystate` alone: private (0700, the gateway's user,
never a symlink), marked as the gateway's by a marker written through a staged
file, apart from evidence storage, and confers no authority. A directory
without the marker is adopted only when it is empty but for the lock file and
staged marker files, each a regular file. The marker, the lock and the mark
are each refused unless they are regular files, the lock is never opened
through a link, and the marker is read only up to its own length. A mark is at
most 64 bytes: a longer one is corrupt, decided by reading at most 65. A run holds it exclusively (`gatewaystate.Acquire`, an OS
lock taken once the marker is in place and before the mark is read, released
by the OS if the process dies), so a second gateway on the same directory is
refused and two clocks never share a mark. The mark is read nowhere else: no
evidence queue directory is consulted. A mark that is not a regular file, not
the contract's decimal grammar, or at or above the ceiling
(`mqttauth.SigningCeiling`, 2^63 − 1 − 2,001) refuses the start. Each value is
the latest of the wall clock, one past the last value issued and one past the
mark found. Before issuing anything the start persists the later of the mark
found and the wall clock plus 1,000 ms, and refuses if that write fails or has
not completed within 1,000 ms; every later mark is the later of the wall clock
plus 1,000 ms and the value being issued, never measured from an earlier mark,
so restarts accumulate no lead. Marks are extended in the background, so
signing never waits on the disk within a reservation, and only an extension
that would move the mark forward starts, so after a step back the coverage
already persisted is used up first. A write completes only once its staged
file is synced, renamed over the mark and the directory synced; the covered
value is the highest completed mark. At most one write is outstanding, no
write runs under the lock that issues values, and no mark is lower than one
written before, whatever that write's outcome. A value past the covered value
waits for a write, no longer than `mqttauth.MarkWriteBound` (1,000 ms,
monotonic) in all and never past the moment an episode opens. An episode opens
when a write fails, is outstanding past the bound, or a value has waited that
long: it is warned about once, the heartbeat and site health report `degraded`
without waiting on the write, no value waits on the disk, and the write is
retried no sooner than 1,000 ms after the previous one started, never per
message. It closes, under the issuing lock, only on a completion covering the
last value issued. An episode is not a GW-6 heartbeat failure. The clock's log records are decided
under its lock, stamped then, and written in that order by one drainer off
every lock, so no caller waits on the logger; at most 64 wait behind a
logger that blocks, later ones are counted, and the refusal after a stop is
logged once. In the gateway the logger is its shared queue (GW-6), which
never blocks: the records wait there among the 1,024 and a drop is counted
there, so under a long stall other components' lines can crowd out the
clock's. A start whose
first value would reach the ceiling is refused, and `Next` refuses a value at
it (`ErrSigningClockExhausted`) or once stopped (`ErrSigningClockClosed`).
Rule 9's upper bound is judged against the wall clock read when a mark is
chosen. The state directory must be on persistent local storage
(gateway-config/v2 rule 14), which the gateway does not check. A signing time
first more than 300,000 ms ahead of the wall clock is logged as a warning. Verified by
`TestReplayVectorCases`, `TestAKeyIsKeptUntilItsEnvelopeIsStale`,
`TestADroppedKeyRaisesTheFloor`, `TestAStartFloorRefusesWhatWasSignedBefore`,
`TestFreshnessReasonsAreNamedApart`, `TestSignersNeverRepeatASignedAt`,
`TestHeartbeatsNeverShareASigningTime`,
`TestARestartLoopNeverAccumulatesALead`,
`TestTheStartWritesTheMarkItFound`, `TestAStartThatCannotWriteIsRefused`,
`TestTheClockNeverReachesTheCeiling`,
`TestAWriteNeverLandsBelowAValueAlreadyIssued`,
`TestEveryMarkStaysWithinItsBounds`, `TestAFailedWriteIsRetriedOnItsPace`,
`TestEachFailureEpisodeIsWarnedAboutAgain`,
`TestAnExtensionThatFailsOpensAnEpisode`,
`TestAStalledWriteOpensAnEpisodeAtTheWriteBound`,
`TestAValueWaitsOnlyTheRemainingDeadlineOfTheOutstandingWrite`,
`TestAValueThatWaitedTheWriteBoundOpensAnEpisode`,
`TestRetriesArePacedFromThePreviousWritesStart`, `TestALateFailureThenRecovery`,
`TestAMarkNeverLowersOneAlreadyWritten`, `TestAStartWhoseFirstWriteStallsIsRefused`,
`TestCloseWithinReportsAWriteStillOutstanding`,
`TestParallelSignersAcrossAStalledReservation`,
`TestAStalledMarkWriteDegradesTheHeartbeatWithoutStoppingIt`,
`TestSiteHealthReportsAnEpisodeWithoutWaitingOnTheWrite`,
`TestTheHeartbeatThatOpensAnEpisodeReportsIt`,
`TestAStopDuringAnEpisodeStartsNoWrite`, `TestAWriteFailingAfterStopArmsNoRetry`,
`TestALoggerThatBlocksNeverHoldsUpSigning`,
`TestLogLinesKeepTheOrderTheyWereDecidedIn`,
`TestLogLinesBehindABlockedLoggerAreBounded`,
`TestTheClocksRecordsHonourTheLoggersLevel`,
`TestTheAheadWarningIsAtThreeHundredThousandMilliseconds`,
`TestAFloorWarningNeverHoldsAnotherVerification`,
`TestNoMarkReachesTheCeilingNearIt`,
`TestAJumpPastTheReservationReservesAhead`,
`TestExtensionsKeepALeadAsTheWallClockMoves`,
`TestAStateDirectoryThatRefusesStopsTheStartBeforeTheBroker`,
`TestAMarkIsAtMostSixtyFourBytes`,
`TestARollbackUsesUpTheCoverageAlreadyPersistedFirst`,
`TestARestartResumesAboveEveryReservation`,
`TestTheValueOnePastAReservationIsPersisted`,
`TestASigningClockAheadOfTheWallIsReported`,
`TestAClosedClockRefusesToSign`,
`TestSigningNeverWaitsOnTheDiskWithinAReservation`,
`TestEverySignerSharesTheGatewaysSigningClock`,
`TestASignedAtNeverRepeatsAcrossARestart`,
`TestAMarkInTheEvidenceQueueDirectoryIsNeverRead`, the
`internal/gatewaystate` tests, and, for the load rules, the
`gateway-config/vectors/state-directory-v2.json` corpus through
`TestStateDirectoryVectors`.
