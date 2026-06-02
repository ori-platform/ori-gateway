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
