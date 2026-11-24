# Architecture Decision Records

The topic documents in `docs/architecture/` describe the system as it *is*; ADRs record
*why* cross-cutting decisions were made at the time, including the alternatives that
lost. Topic docs are updated in place; ADRs are immutable once accepted and superseded
by newer ones instead of edited.

## Format

One file per decision: `NNNN-kebab-case-title.md`, numbered sequentially. Sections:

- **Status** — `Proposed` → `Accepted` → `Superseded by NNNN`
- **Context** — the problem and the forces acting on it
- **Decision** — what was decided, in active voice
- **Consequences** — what becomes easier, harder, or committed-to
- **Alternatives considered** — what lost, and why

## Index

| ADR | Title |
|-----|-------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions |
| [0002](0002-license-bound-to-contact-id.md) | License bound to the ContactId, keys as rotatable authenticators |
| [0003](0003-hybrid-signatures-for-license-certificates.md) | Hybrid signatures for license certificates |
| [0004](0004-fail-open-entitlement-evaluation.md) | Fail-open entitlement evaluation |
| [0005](0005-web-activation-zero-pii.md) | Web-side activation with zero PII |
| [0006](0006-license-server-placement-transport-replay.md) | License server placement, transport, replay protection, and operations |
| [0007](0007-push-stream-as-the-state-channel.md) | The push stream is the state channel, and the wire keeps its replies |
| [0008](0008-host-run-conditions-for-internet-mode.md) | Internet mode runs on host conditions, decided below Flutter |
| [0009](0009-contact-id-derived-from-identity-keys.md) | The ContactId is derived from the identity's public keys |
| [0010](0010-per-user-listen-port.md) | Each user listens on a port of their own, chosen in the database |
| [0011](0011-file-data-on-streams.md) | File data travels on one authenticated stream per attempt |
| [0012](0012-password-encrypted-backups.md) | Backups are encrypted with a password the device remembers as a key |
| [0013](0013-linux-through-nix.md) | Fyles reaches Linux through Nix, as prebuilt binaries |
