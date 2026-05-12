# Ori Design Principles

This repository is part of the Ori platform. Every design decision must survive
the eight lenses below. Principles 1-7 are inherited from
[`ori-runtime` PRINCIPLES.md](https://github.com/ori-platform/ori-runtime/blob/main/PRINCIPLES.md). Principle 8 governs cross-repo integration.

## 1. Actuation Trust

Physical trust is earned, not assumed.

## 2. Constraint

Designed for the world's majority condition.

## 3. Inviolable Safety

The deterministic Tier D layer is absolute.

## 4. Assumed Failure

Hardware lies. Software freezes. Power dies.

## 5. Offline-First

Offline-first is the requirement, not a feature.

## 6. Explicit Capability

The skill is the atomic unit of trust.

## 7. Explicit Authority Boundaries

Learning improves reasoning, never grants permissions.

## 8. Contract Fidelity

Consume [`ori-runtime`](https://github.com/ori-platform/ori-runtime) and [`ori-specs`](https://github.com/ori-platform/ori-specs) contracts. Never reinvent them.

Repo-specific note:
`ori-gateway` coordinates a site. It may improve reasoning and aggregate LAN
context, but it must never become a dependency for [runtime](https://github.com/ori-platform/ori-runtime) Tier D safety.
