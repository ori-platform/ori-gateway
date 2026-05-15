## What does this PR do?

<!-- Describe the change. Focus on WHY, not just what. -->

## Type of change

- [ ] `feat` - new gateway package, provider, dispatcher, broker, SIM/fleet, or site feature
- [ ] `fix` - bug fix or contract correction
- [ ] `docs` - documentation only
- [ ] `test` - tests only
- [ ] `refactor` - no behavior change
- [ ] `security` - touches secrets, provider auth, SIM, fleet, or request authority
- [ ] `contract-change` - changes MQTT topics, payloads, or SDK/runtime-facing contracts

## Required checklist

- [ ] Linked issue is included below and acceptance criteria are addressed
- [ ] `go test ./...` passes
- [ ] `go vet ./...` passes
- [ ] Pre-commit passes for changed files
- [ ] Every new `.go` file has the Apache-2.0 license header
- [ ] `gateway.yaml.example` is updated if config changed
- [ ] Contract fixtures are updated if request, response, heartbeat, or topic behavior changed

## Gateway invariant checklist

- [ ] MQTT topics match `ori-specs/gateway-api/v1.md` exactly
- [ ] Every reasoning response preserves the original `request_id`
- [ ] Provider timeout/error paths return an error response; they do not leave requests unanswered
- [ ] Heartbeat publication cannot fail silently
- [ ] `sim.enabled=false` performs zero modem initialization or serial probing
- [ ] `fleet.enabled=false` performs zero HTTP, DNS, or auth work

## Provider or network checklist

Complete if this PR touches MQTT, providers, HTTP, SIM, or fleet.

- [ ] Context cancellation/timeouts are tested
- [ ] Secrets and API keys never appear in logs or errors
- [ ] Reconnect/degraded paths have negative tests where applicable
- [ ] Disabled optional modules are tested as inert

## If you used AI assistance

- [ ] I can explain every line of AI-generated code in this PR
- [ ] I have read and understood every file I modified
- [ ] I am not submitting code I cannot defend in review

## Related issue

<!-- Closes #<issue-number> -->

## Testing notes

<!-- Include commands run and any intentionally skipped checks. -->
