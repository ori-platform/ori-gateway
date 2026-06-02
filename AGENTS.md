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

10. `GW-10` Product reporting providers are separate from Tier 3 reasoning providers.
The gateway may own connected customer-reporting and enrichment providers, but
those providers must use reporting-specific config and must not be wired into
the Tier 3 reasoning provider factory.

11. `GW-11` Gateway reporting and enrichment never change action authority.
Customer-facing weekly reports and Tier C explanation enrichment are advisory.
They must not promote, downgrade, approve, reject, bypass, or execute runtime
action tiers. Runtime remains the physical action authority.

12. `GW-12` Reporting provider credentials stay out of runtime config.
Gemini/API keys and equivalent product-provider credentials belong in gateway
or product environment variables only. Secret values must never be committed,
and runtime config examples must remain provider-neutral.

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
