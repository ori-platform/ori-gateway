# Contributing

Contributions should preserve the gateway's role as a LAN site coordinator.

Before opening a PR:

```bash
go test ./...
gofmt -w .
```

## Review Rules

- Keep Gateway API payloads aligned with [`ori-specs/gateway-api/v1.md`](https://github.com/ori-platform/ori-specs/blob/main/gateway-api/v1.md).
- Do not introduce [`ori-runtime`](https://github.com/ori-platform/ori-runtime) imports or runtime-specific persistence.
- Do not add cloud calls unless the feature is explicitly behind fleet config.
- Do not add code that changes Tier D behavior.
- Add tests for topic names, request correlation, timeout behavior, and heartbeat
  semantics when those areas change.

## Commit Style

Use conventional commits:

```text
feat(gateway): add mqtt request subscriber
fix(provider): preserve request id on timeout response
docs(security): clarify heartbeat threat model
```
