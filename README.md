# ori-gateway

LAN gateway and site coordinator for Ori deployments.

[`ori-runtime`](https://github.com/ori-platform/ori-runtime) is the brain of one device. The gateway is the coordinator for one
site. [`ori-cloud`](https://github.com/ori-platform/ori-cloud) is the commercial backend for a fleet. Each layer has a
distinct job and must not absorb the responsibilities of the others.

## Three Purposes

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
- LAN health heartbeat publisher with supervision
- Gateway process wiring: config, provider, broker, heartbeat, dispatcher, and graceful shutdown
- Runtime export client contracts and MQTT runtime export client
- Runtime health posture mapping for broker hardening, state-store encryption, and alert outbox backlog
- Weekly report generation against runtime export interfaces
- Tier C enrichment contracts
- SIM and fleet optional-module stubs with disabled-path safety guarantees
- CI, repository invariants, and contribution guardrails

Deferred implementation:

- Full SIM modem integration for shared outbound SMS
- SMS webhook signing bridge for provider ingress before forwarding to runtime localhost webhooks
- Fleet forwarding and control-plane integration through `ori-cloud`
- Runtime-side Tier 3 gateway reasoning client and deterministic escalation policy
- Runtime-side consumption of gateway heartbeat capability posture

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

## License

Apache-2.0
