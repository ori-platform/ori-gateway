# Security Policy

`ori-gateway` is a LAN service that can influence [runtime](https://github.com/ori-platform/ori-runtime) reasoning and site
coordination. It must never weaken [`ori-runtime`](https://github.com/ori-platform/ori-runtime) safety invariants.

## Supported Versions

| Version | Supported |
| ------- | --------- |
| `0.1.x` | Yes |

## Reporting a Vulnerability

Use GitHub's private vulnerability reporting for this repository:

1. Go to the repository `Security` tab.
2. Click `Report a vulnerability`.
3. Submit details privately.

If private reporting is unavailable, contact the repository owner directly via GitHub.

Do not open public issues for undisclosed vulnerabilities.

## What to Include

Please include:

- Affected component and file paths
- Reproduction steps
- Impact on [runtime](https://github.com/ori-platform/ori-runtime) reasoning, heartbeat availability, or site coordination
- Whether request/response correlation can be broken
- Whether the gateway can influence Tier D safety behavior
- Suggested remediation, if available

## Highest Priority Findings

- Gateway response `request_id` mismatch or spoofing
- MQTT topic drift from [`ori-specs/gateway-api/v1.md`](https://github.com/ori-platform/ori-specs/blob/main/gateway-api/v1.md)
- Gateway availability falsely reported as healthy
- Provider failures that leave [runtime](https://github.com/ori-platform/ori-runtime) requests unanswered
- Any code path that attempts to control or block Tier D behavior
- Unintended cloud calls when fleet mode is disabled
- Secrets exposure in config, logs, fixtures, or CI

## Response Targets

- Initial acknowledgment: within 72 hours
- Triage and severity decision: within 7 days
- Critical/high patch target: usually within 14 days

## Safe Harbor

Good-faith security research is welcome when disclosed privately and performed
against systems you own or have permission to test.
