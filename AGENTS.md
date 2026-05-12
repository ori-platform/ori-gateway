# AGENTS.md - ori-gateway

This repository implements the LAN gateway and site coordinator for Ori.

## Purpose

`ori-gateway` has three responsibilities:

1. Provide Tier 3 LAN reasoning for [`ori-runtime`](https://github.com/ori-platform/ori-runtime) devices.
2. Publish a LAN health heartbeat for [`ori-runtime`](https://github.com/ori-platform/ori-runtime) capability posture.
3. Coordinate multi-device site context.

It is not [`ori-runtime`](https://github.com/ori-platform/ori-runtime) and not [`ori-cloud`](https://github.com/ori-platform/ori-cloud).

## Invariants

1. `GW-1` Topic names must match [`ori-specs/gateway-api/v1.md`](https://github.com/ori-platform/ori-specs/blob/main/gateway-api/v1.md) exactly.

2. `GW-2` Every response must echo the request `request_id`.
A missing or changed `request_id` causes [`ori-runtime`](https://github.com/ori-platform/ori-runtime) timeout and fallback behavior.

3. `GW-3` Providers must satisfy one shared interface.
Provider-specific settings belong in config, not in the provider interface.

4. `GW-4` Gateway never changes action authority.
[`ori-runtime`](https://github.com/ori-platform/ori-runtime) owns action-tier authority. Gateway may echo tier hints but must not
promote or downgrade physical authority.

5. `GW-5` Gateway is not in the Tier D path.
Tier D safety fires locally in the [runtime](https://github.com/ori-platform/ori-runtime) rule engine before gateway, cloud, or
network systems are consulted.

6. `GW-6` Heartbeat must be reliable.
If heartbeat publication cannot continue, the gateway must report failure or
exit rather than silently appearing available.

7. `GW-7` Fleet/cloud forwarding is opt-in.
With fleet disabled, gateway must not call `ori-cloud`, resolve cloud hosts, or
attempt authentication.

8. `GW-8` Site coordination is LAN-scoped.
Cross-device correlation and shared GSM are site functions. Fleet analytics and
billing belong in `ori-cloud`.

9. `GW-9` Gateway must not persist reasoning results.
Stateful learning and causal memory belong in [runtime](https://github.com/ori-platform/ori-runtime) or cloud-defined stores, not
in the gateway request proxy.

## Layout

```text
cmd/ori-gateway/
internal/contracts/
internal/provider/
internal/session/
internal/site/
```

## Verification

```bash
go test ./...
gofmt -w .
```
