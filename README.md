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

## Bootstrap Scope

Implemented in this repository baseline:

- Gateway API v1 typed request/response contracts
- Topic helpers for request/response/heartbeat topics
- Provider interface for reasoning backends
- Request/response correlation validation
- Session registry primitives for request lifecycle tracking
- Site heartbeat aggregation primitives
- CI, repository invariants, and contribution guardrails

Deferred implementation:

- MQTT broker connection loop
- llama.cpp provider
- Claude provider
- SIM module integration
- Fleet forwarding to ori-cloud

## Invariant

The gateway is never in the Tier D path. Tier D fires locally in the
[`ori-runtime`](https://github.com/ori-platform/ori-runtime) rule engine before gateway, cloud, or network systems are consulted.

## Development

```bash
pre-commit install
go test ./...
go fmt ./...
```

## License

Apache-2.0
