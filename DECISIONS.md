# DECISIONS.md — ori-gateway

This file records accepted architecture decisions for `ori-gateway`. Decisions
here are meant to preserve runtime/gateway boundaries and prevent future
implementation drift.

---

## 2026-06-02 — Gemini Integration Boundary

**Status:** Accepted

Gemini belongs in the gateway and product layer for reporting and enrichment,
not in the runtime safety core.

Rules:

- `ori-runtime` remains offline-first and provider-neutral. It must not gain a
  Google/Gemini SDK dependency for product reporting.
- `ori-gateway` may own connected reporting and enrichment providers because the
  gateway already assumes network availability for LAN/cloud coordination.
- Tier 3 reasoning provider selection is separate from product reporting
  provider selection. The `provider` config section is for gateway reasoning
  requests; reporting must use its own config section.
- Weekly reports and Tier C explanation enrichment are customer-facing advisory
  features. They must not promote, downgrade, bypass, or otherwise modify
  runtime action-tier authority.
- Gemini/API keys belong in gateway or product environment variables. Secret
  values must never be committed and must not appear in runtime config examples.
- Gateway reporting/enrichment failure must degrade gracefully. It must not block
  Tier C approval handling, Tier D safety, or runtime fallback behavior.

Rationale:

- Runtime controls physical actuation and must remain capable under degraded or
  offline connectivity.
- Gateway is the right boundary for connected product intelligence because it
  can aggregate site data while keeping runtime storage and actuation semantics
  intact.
- Separating reasoning providers from reporting providers prevents cloud
  reporting features from drifting into the Tier 3 request proxy or Tier D
  safety paths.
- Keeping API keys outside runtime config preserves the framework boundary and
  reduces the blast radius of product-layer credentials.

---

## 2026-06-03 — Tier C Enrichment Is Advisory Only

**Status:** Accepted

Tier C enrichment improves the operator-facing explanation for an existing
runtime proposal. It is not a decision path and it is not an action-authority
path.

Rules:

- Enrichment requests may include proposal context, reading context, recent
  history, proposed action, and safe default action so the provider can explain
  the situation clearly.
- Enrichment responses may return explanation text, estimated impact, and
  operator context only.
- Enrichment responses must not contain fields that change action tier, action
  name, safe default action, approval requirement, relay mode, actuator state, or
  any other runtime authority field.
- Runtime must preserve the original proposal if enrichment fails or times out.
- Gateway enrichment must not execute actions or mutate runtime state.

Rationale:

- Tier C approval is a safety boundary. Connected language enrichment can make an
  operator message clearer, but it cannot become an alternate approval or action
  selection channel.
- Keeping the response schema advisory-only lets product intelligence improve
  customer comprehension without weakening runtime safety invariants.

---

## 2026-06-11 — Gateway Heartbeat Signing and Broadcast Trust Model

**Status:** Accepted

The gateway publishes `ori/gateway/health` as the sole runtime-side
`gateway_reachable` signal. Runtimes must not infer gateway reachability from
public internet checks, REST probes, runtime node heartbeats, or export traffic.
The effective gateway-loss detection window is:

```text
heartbeat_interval_s + runtime gateway_heartbeat_ttl_seconds
```

Tune both values together. A longer interval reduces LAN chatter but increases
the time a runtime may still attempt Tier 3 escalation after the gateway has
stopped publishing. Tier D and trigger-authoritative action tier rules remain
independent of this signal.

When `gateway.auth.enabled: true`, heartbeat payloads are signed with the same
HMAC envelope shape as other runtime-gateway MQTT messages, but with the
broadcast trust model:

```text
message_type = "gateway.heartbeat"
device_id = ""
request_id = ""
signed_at_ms = heartbeat.timestamp_ms
canonical_json = sorted-key JSON heartbeat payload without auth
```

The topic is site-wide, not device-scoped, so adding `device_id` binding to the
heartbeat signature would break the LAN broadcast contract. Replay protection is
handled by the runtime using `message_type + signed_at_ms + signature`.

When `gateway.auth.enabled: false`, existing unsigned heartbeat behavior is
preserved for initial LAN setup, but production deployments should enable HMAC
and broker ACLs.

Related: ori-runtime#144, ori-runtime#145, gateway #43.

---

## 2026-06-11 — Runtime Node Heartbeat Consumption

**Status:** Accepted

The gateway subscribes to concrete `ori/{device_id}/runtime/heartbeat` topics
for devices listed in `gateway.device_ids` and updates its site registry from
runtime-published node liveness payloads. Runtime node
heartbeat is gateway infrastructure, not sensor data, so it is consumed by a
site registry handler directly and does not pass through any sensor/event bus or
the reasoning dispatcher.

When `gateway.auth.enabled: true`, runtime node heartbeat payloads must verify
with the dedicated runtime-gateway HMAC secret using:

```text
message_type = "runtime.heartbeat"
device_id = topic device_id
request_id = ""
signed_at_ms = auth.signed_at_ms
canonical_json = sorted-key JSON heartbeat payload without auth
```

Invalid, retained, stale, replayed, mismatched, or malformed heartbeat payloads
are rejected and logged without stopping the gateway. Subscription failure is fatal
at startup because the gateway cannot maintain reliable site liveness without
the runtime-node heartbeat stream.

Registry eviction treats future-dated `last_seen_ms` values as stale. A node
with a bad forward clock must not become immortal in the site registry. Far-future
`last_seen_ms` values are rejected at ingest and future-dated entries are also
evicted defensively during registry sweeps.

Related: ori-runtime#145, ori-gateway#45.

---

## 2026-06-14 — Runtime Health Posture Fields Are Gateway-Visible

**Status:** Accepted

The gateway consumes runtime health through runtime-owned MQTT export contracts.
The runtime now exposes deployment posture fields for broker hardening,
state-store encryption-at-rest posture, and alert outbox backlog health. The
gateway maps these into typed runtimeclient fields rather than treating them as
opaque JSON.

This preserves the boundary that gateway must not read runtime SQLite or runtime
configuration files directly. Runtime remains the source of truth for local
posture; gateway and future cloud/product layers can surface site risk from the
health export.

Non-goals:

- This does not prove the runtime broker posture declarations are true.
  Deployment tooling still needs to validate Mosquitto or broker config where it
  has access to broker files/admin APIs.
- This does not implement the SMS webhook signing bridge. That remains a
  gateway/deployment ingress feature.

---

## 2026-06-14 — SMS Webhook Signing Bridge Owns Provider Ingress

**Status:** Accepted

The runtime requires raw-body HMAC verification for public SMS webhook ingress,
but providers such as Africa's Talking do not emit Ori HMAC headers. The gateway
therefore owns an optional webhook signing bridge for production provider
ingress.

Rules:

- The bridge validates provider source CIDRs before reading or forwarding the
  request body.
- The bridge preserves the provider raw body exactly, signs that raw body using
  the runtime webhook HMAC contract, and forwards it to the runtime localhost
  webhook with `X-Ori-Webhook-*` headers.
- Bridge token and HMAC secret values come from environment variables only.
- The bridge caps request bodies and maps runtime rejection to provider-facing
  failure without exposing runtime internals.
- The bridge must not log SMS body content, phone numbers, bearer tokens, or
  HMAC secrets.
- Gateway heartbeat may publish bridge readiness and bounded posture fields.
  Readiness is a live bridge-loop signal, not a static config assertion. The
  heartbeat must not publish target URLs, env var names, provider CIDR values,
  token values, HMAC secrets, or webhook body content.

Rationale:

This keeps the runtime webhook security model strict without requiring every
third-party SMS provider to support Ori-specific signatures. It also preserves
the runtime boundary: the gateway adapts provider ingress, while the runtime
continues to verify the same replay-resistant raw-body signature contract.

## 2026-06-14 — Gateway Runtime Export Auth and Encryption Parity

Status: Accepted

The gateway runtime export client signs `export_request` payloads when
`gateway.auth.enabled` is true and verifies `export_response` payloads before
mapping them into reporting data. Sensitive runtime exports are decrypted when
`gateway.encryption.enabled` is true, using the same gateway shared secret,
HKDF-SHA256 domain label, AES-256-GCM scheme, and authenticated metadata used
by the runtime.

Rules:

- New outbound gateway signatures use `gateway.auth.shared_secret_env` only.
- `gateway.auth.previous_shared_secret_env` is verify-only for zero-downtime
  rotation.
- Sensitive export responses must be HMAC-verified before AES-GCM decryption.
- Health exports may remain plaintext operational posture.
- Gateway encryption cannot be enabled unless gateway auth is enabled.

Rationale:

The runtime production posture can require authenticated gateway MQTT and
encrypted sensitive exports. Gateway reporting and future cloud sync must work
through that same contract instead of requiring operators to weaken runtime
security for product features.

---

## 2026-06-14 — Scheduled Weekly Reports Use Runtime Export Contracts

Status: Accepted

The gateway owns single-site weekly report generation. A scheduled weekly report
uses an explicit `reporting.weekly_report` scope (`device_id`, `sensor_ids`,
customer/site labels, local weekday/time/timezone), fetches bounded runtime
exports through `runtimeclient.Client`, and delegates language generation to the
configured reporting provider. Gemini is the first concrete reporting provider.

Rules:

- Gateway weekly reports must never read runtime SQLite directly.
- Gateway weekly reports must never mutate runtime state or action authority.
- Runtime export MQTT auth/encryption posture applies to report data access.
- Report generation failures are logged and retried on the next schedule; they
  must not stop Tier 3 reasoning, heartbeat, runtime-node liveness, or webhook
  bridge handling.
- Delivery and persistence of generated report text belong to product/cloud
  surfaces and are intentionally not implemented in this gateway slice.

Rationale:

The weekly report is a product feature, not a safety or action path. Keeping it
on the gateway lets a site generate useful customer-facing intelligence from
local runtime data while preserving runtime ownership of state and actuation.

---

## 2026-06-14 — Gateway Runner Supervision Is Centralized

Status: Accepted

The gateway process now supervises heartbeat, webhook bridge, weekly report, and
future long-running app tasks through one `errgroup`-backed supervisor. Startup
code registers runners by name, and shutdown waits through a single path instead
of maintaining one channel and one drain call per runner at every early return.

Rules:

- Required long-running app loops must be registered with the shared
  `errgroup`-backed supervisor once constructed.
- A runner error before process cancellation remains fatal and is reported with
  the runner name.
- A runner returning nil before process cancellation remains an unexpected-stop
  error.
- A runner returning nil or `context.Canceled` after process cancellation is a
  clean shutdown.
- Future runners must not reintroduce ad hoc `fooErr` channels, sized result
  buffers, or repeated manual drain calls in `runGateway`.

Rationale:

The gateway now has multiple supervised loops. Keeping shutdown supervision on
`errgroup` prevents future optional modules from leaking goroutines or broker
resources when startup fails after some runners have already begun, without
fixed-size result buffers or call-site drain counters.

---

## 2026-06-15 — Weekly Reports Surface Non-Secret Runtime Posture

Status: Accepted

Weekly report generation may include runtime health posture fields that are safe
for customer-facing operational summaries: gateway broker hardening posture,
state-store encryption posture, and alert outbox backlog health. These fields
come only from the runtime health export through `runtimeclient.Client`.

Rules:

- Reports must not read runtime SQLite files or runtime config directly.
- Reports must not expose remote-command sender identities, lockout risk details,
  secrets, provider credentials, MQTT URLs, or filesystem paths.
- Degraded posture is surfaced as report warnings so Gemini/reporting providers
  can explain reliability risk in plain language.

Rationale:

The weekly report is a product-facing proxy for site health. It should explain
operational risk that affects customer trust, but it must preserve the runtime
boundary and avoid leaking security-sensitive implementation details.

---

## 2026-06-15 — Site Health Projection Owned by the Gateway

Status: Accepted

The gateway owns site-level health projection. No equivalent type exists in
`ori-runtime`; the runtime exports per-device `HealthSnapshot` records, and the
gateway's `internal/site` package is responsible for aggregating these into a
multi-device site view.

Rules:

- `internal/site.Projector` is the single point of site health projection. It
  reads only from the node registry (`Registry.Snapshot`) and a caller-supplied
  `GatewayView`; it does not read runtime SQLite files or runtime config.
- `SiteHealth` and `SiteNode` are advisory and read-only. They must not change
  action authority or be used to gate actuation decisions.
- `ActiveTriggers` from node heartbeats are surfaced as a count only
  (`ActiveTriggerCount int`). Trigger-name strings must not appear in projected
  output.
- `GatewayView` must not contain target URLs, MQTT URLs, env var names, bearer
  tokens, HMAC secrets, phone numbers, or filesystem paths.
- Future consumers (ori-cloud sync, dashboard HTTP) must depend on the
  `site.Viewer` interface, not on MQTT internals or the registry directly.
- Webhook bridge enablement and other optional module flags are carried in
  `GatewayView` as booleans only and do not affect the aggregate site status
  computation; they are informational.

Rationale:

The runtime is device-scoped and offline-first. Site-level coordination —
knowing which nodes are present, stale, or missing relative to a configured
fleet — is a gateway responsibility. Keeping this projection in `internal/site`
prevents it from being scattered across reporting, fleet, or heartbeat packages
and makes the secrets boundary explicit and testable.

---

## 2026-06-15 — Weekly Report Delivery Architecture

**Status:** Accepted

Report delivery is separated from report generation via a narrow `Deliverer`
interface so that new output channels can be added without touching generation
or scheduling logic.

Rules:

- `Deliverer` has a single method: `Deliver(ctx, WeeklyReportArtifact) error`.
  Implementations must not mutate the artifact.
- `LogDeliverer` is always wired. It is the default when `RunnerOptions.Deliverers`
  is empty and is explicitly prepended in `app.go` when wiring from config.
- `FileDeliverer` is optional: it is added only when `output_dir` is a non-empty
  absolute path in `reporting.weekly_report`. It never creates directories; the
  operator must ensure the directory exists.
- Delivery errors are advisory. The runner logs them at Warn level and continues
  to the next schedule tick. A failed delivery must never stop the runner or
  skip remaining deliverers in the same tick.
- `WeeklyReportArtifact` is the only type exposed to `Deliverer` implementations.
  It must not contain MQTT URLs, broker credentials, filesystem paths, phone
  numbers, bearer tokens, HMAC secrets, or lockout risk details.
- `output_dir` must be an absolute path with no `..` segments. Config validation
  enforces this at load time. `FileDeliverer.Deliver` enforces the absolute-path
  invariant again at call time as a defence-in-depth check.
- The filename written by `FileDeliverer` is `weekly-{site_slug}-{YYYY-MM-DD}.json`
  where the date is derived from `WindowEndMS` in UTC. The slug function lowercases
  the site name and replaces non-alphanumeric characters with underscores,
  collapsing consecutive underscores.

Rationale:

Separating generation from delivery keeps the `WeeklyReportRunner` testable
without a filesystem and lets the file output channel be added or removed
independently. Defaulting to `LogDeliverer` preserves existing behaviour for
all callers that pre-date delivery config.

---

## 2026-06-15 — Weekly Report Delivery Config Shape

**Status:** Accepted

`reporting.weekly_report.delivery` is a nested block, not a flat field, because
ori-cloud report persistence is the next backend and adding it later as a peer
field (e.g. `output_dir` + `cloud_endpoint`) would produce an incoherent shape.

Rules:

- All delivery channels live under `weekly_report.delivery.*`.
- `delivery.file` is the local operator artifact channel (disk write).
- `delivery.cloud` is the ori-cloud persistence channel (HTTPS push).
- `log` delivery is always active and requires no config.
- Future channels (`email`, `webhook`) are added as sibling keys under
  `delivery`, not as flat fields on `WeeklyReportConfig`.
- The same `weeklyReportFilePayload` DTO (excluding DeviceID and Metadata) is
  the canonical customer-safe payload shape for both file and cloud delivery.
- `delivery.cloud.auth_env` holds only an environment variable name, never a
  credential. The gateway resolves it at startup.
- Cloud delivery returns "not yet implemented" at startup until the ori-cloud
  ingest endpoint is built. The config shape and validation are already in place.

Rationale:

Establishing the nested shape before ori-cloud ships avoids a config migration
later and makes the delivery model explicit: `log` is observability, `file` is
a local debug artifact, `cloud` is the authoritative persistence path.

---

## 2026-06-15 — Site/Customer Identity is Provisional in Gateway Config

**Status:** Accepted (provisional)

`customer_name` and `site_name` in `reporting.weekly_report` are local display
labels copied into the gateway config at install time. They are **not** the
long-term identity authority and must not be used as persistence or routing keys.

Rules:

- `customer_name` and `site_name` are display labels only. They may appear in
  report text and customer-facing file output. They must not be used as primary
  keys for cloud storage, billing, or account lookup.
- Ori-cloud is the authoritative fleet identity registry. It owns the canonical
  mapping of gateway → site → customer → product/subscription.
- When cloud delivery is wired, the gateway must authenticate to ori-cloud with
  a gateway credential. Ori-cloud infers site and customer ownership from that
  credential, not from the names in the report payload.
- The `weeklyReportFilePayload` DTO may carry `customer_name` and `site_name`
  as display labels in cloud-delivered payloads. They are advisory strings for
  rendering, not authoritative identifiers.
- The `fleet` module is the eventual channel for receiving canonical site profile
  data from ori-cloud (site_id, customer_id, display names). Until that sync
  exists, static config is acceptable for local/file delivery.

Rationale:

A gateway config typo in `customer_name` must not cause reports to be stored
under the wrong customer in ori-cloud. Keying persistence on authenticated
gateway identity (rather than human-readable names) prevents drift between
what the operator typed and what ori-cloud has on record. This constraint must
be respected when the cloud deliverer PR is designed.

Deferred to cloud delivery PR:

- Gateway registration / credential issuance model
- Whether ori-cloud returns canonical display names at registration
- Local cache of site profile for offline report generation
- Cloud ingest endpoint contract and payload schema (site_id vs. auth-inferred)
