# 0001 — Record architecture decisions

## Status

Accepted (2026-08)

## Context

Fyles has grown living topic documents under `docs/architecture/`, but they describe the
current state only. With pro licensing, the project takes its first decisions whose
rationale involves external forces (payment providers, app-store rules, hosting costs)
that will not be reconstructable from the code later.

## Decision

Adopt Architecture Decision Records under `docs/architecture/adr/`, using the format
described in [README.md](README.md). Topic docs remain the living description of the
system; ADRs capture the "why at the time" and are superseded, not edited.

## Consequences

- Decisions about the licensing system (0002–0006) have a durable rationale trail.
- Future cross-cutting decisions (new subsystems, protocol changes, hosting moves)
  should add an ADR; routine implementation choices should not.

## Alternatives considered

- **Rationale inside topic docs** — rots when the doc is updated in place; the losing
  alternatives disappear with it.
- **Commit messages only** — good locality, but unsearchable as a decision index and
  invisible to readers of the docs tree.
