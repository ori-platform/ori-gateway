# ori-gateway

LAN gateway and site coordinator for Ori deployments.

[`ori-runtime`](https://github.com/ori-platform/ori-runtime) is the brain of one device. The gateway is the coordinator for one
site. [`ori-cloud`](https://github.com/ori-platform/ori-cloud) is the commercial backend for a fleet. Each layer has a
distinct job and must not absorb the responsibilities of the others.

## Four Purposes

1. Tier 3 LAN reasoning

The gateway runs a larger model on LAN hardware such as a laptop, NUC, or local
server. Runtime devices use it when the Pi-local SLM is not enough and internet
access is unavailable or undesirable.

2. LAN health heartbeat

The gateway publishes `ori/gateway/health` every 30 seconds. Runtime devices use
that heartbeat to decide whether Tier 3 is reachable. This is a LAN capability
signal, not a public internet probe.

3. Site coordination

The gateway aggregates multiple Edge Node heartbeats, supports cross-device
anomaly correlation, and can eventually provide shared site resources such as a
single GSM modem for outbound SMS.

4. Blind evidence courier

The gateway durably accepts exact runtime evidence bytes, issues a dedicated
custody acknowledgement only after that commit, and carries the bytes over an
independently authenticated HTTPS channel. It never holds evidence-device or
authority signing keys and never treats custody as an authority receipt.

## Runtime Contract

This repo implements the Gateway API defined in:

- [`ori-specs/gateway-api/v1.md`](https://github.com/ori-platform/ori-specs/blob/main/gateway-api/v1.md)

Runtime baseline:

- [`ori-runtime`](https://github.com/ori-platform/ori-runtime) `v0.9.0-beta.2+`

## Current Scope

Implemented in this repository:

- Gateway API v1 typed request/response contracts
- Topic helpers for request/response/heartbeat topics
- MQTT broker client with reconnect and fail-fast publish behavior
- Tier 3 reasoning provider interface and provider factory
- Echo and llama.cpp reasoning providers
- Runtime reasoning request dispatcher with timeout/error responses
- Request/response correlation and topic/device validation
- Session registry primitives for request lifecycle tracking
- Site heartbeat aggregation primitives
- LAN health heartbeat publisher with supervision and webhook bridge posture
- Gateway process wiring: config, provider, broker, heartbeat, dispatcher, and graceful shutdown
- Reasoning dispatcher with the runtime-gateway HMAC envelope verified on requests and applied to responses when gateway auth is enabled, and `confidence` emitted in a form both serialisers agree on
- Runtime export client contracts and MQTT runtime export client with HMAC auth and sensitive export decryption
- Runtime health posture mapping for broker hardening, state-store encryption, and alert outbox backlog
- SMS webhook signing bridge for providers that cannot sign raw webhook bodies
- Scheduled weekly report generation against runtime export interfaces, with Gemini as the first reporting provider
- Weekly report delivery via log, file, and HTTPS cloud deliverers
- Tier C enrichment contracts and handler, with the runtime-gateway HMAC envelope verified on requests and applied to responses when gateway auth is enabled
- Durable outbound evidence and authority-return queues, authenticated custody,
  an isolated persistent evidence MQTT session, and the independent authority
  HTTPS channel defined by `gateway-api/v1` and `evidence-transport/v1`
- SIM and fleet optional-module stubs with disabled-path safety guarantees
- CI, repository invariants, and contribution guardrails

Consumed by `ori-runtime` (implemented there):

- The runtime Tier 3 gateway reasoning client and deterministic escalation policy.
- Runtime consumption of the gateway heartbeat capability posture (via the
  `gateway_heartbeat_ttl_seconds` reachability window).

Deferred implementation:

- Full SIM modem integration for shared outbound SMS
- Fleet forwarding and control-plane integration through `ori-cloud`
- Production integration of weekly report delivery with the live `ori-cloud`
  report service — the log, file, and HTTPS cloud deliverers exist; wiring to
  the production service is pending

## Invariant

The gateway is never in the Tier D path. Tier D fires locally in the
[`ori-runtime`](https://github.com/ori-platform/ori-runtime) rule engine before gateway, cloud, or network systems are consulted.

The gateway also does not read runtime SQLite directly. Runtime data used for
reports, enrichment, or site status must come through runtime-owned export
interfaces.

## Development

```bash
pre-commit install
go test ./...
go vet ./...
```

## Versioning and releases

The gateway is independently deployable, so it carries its own
[SemVer](https://semver.org) line rather than tracking another repository's
version. A tag is a distribution label, not a correctness anchor: the first tag
should mark a supported deployment, not administrative symmetry with the other
repositories.

`ori-gateway --version` reports build provenance. An ordinary build already
embeds the source commit and dirty state through Go's module VCS metadata:

```bash
go build ./cmd/ori-gateway && ./ori-gateway --version
```

Release builds inject explicit values without changing that fallback:

```bash
go build -ldflags "\
  -X main.version=v2.1.0 \
  -X main.commit=$(git rev-parse HEAD) \
  -X main.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  ./cmd/ori-gateway
```

## License

Apache-2.0
