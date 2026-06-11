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
