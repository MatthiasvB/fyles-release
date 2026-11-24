# 0003 — Hybrid signatures for license certificates

## Status

Accepted (2026-08)

## Context

License certificates ([0002](0002-license-bound-to-contact-id.md)) need a
server signature clients can verify offline for a decade — a one-time purchase must
still validate long after issuance. The app already ships a hardened hybrid
dual-signature scheme (Ed25519 + ML-DSA-87) for contact identities and session
establishment, including a generic signed-payload envelope (`Signed<P>` in
`p2p/src/crypto.rs`) whose signing input is derived from `Serialize`, so newly added
fields cannot escape authentication.

Anti-tamper is explicitly a non-goal (the client fails open anyway), so the choice is
about reuse and longevity, not about resisting a motivated local attacker.

## Decision

- The license server's keypair **is a `crypto::ContactKeys`** — the same type, seeds,
  and serde adapters as every other identity in the system.
- Certificates and license-protocol requests are dual-signed via a generic
  `crypto::signed::Signed<P>`, **promoted from the p2p crate into the `crypto` crate**
  (`fyles_service/crypto/src/signed.rs`) so that `license_format` and the license server
  never depend on the libp2p stack. Verification uses `verify_strict` for Ed25519 and
  requires both schemes to pass, matching the p2p semantics exactly.
- The envelope exists **exactly once**: `p2p/src/crypto.rs` re-exports
  `crypto::signed::{Signed, PqSignatureWrapper, SignatureVerificationError}` rather than
  defining its own, so there is no second copy to drift.

## Consequences

- No new cryptographic code path to review; the license system inherits the properties
  the session-establishment audit already hardened.
- Post-quantum coverage for decade-lived certificates comes for free.
- ~4.6 KB of signature per certificate — irrelevant for a locally cached blob.

## Alternatives considered

- **Ed25519-only server signatures** — smaller and simpler, but it would be a *new*
  single-scheme code path next to a hardened dual-scheme one, and it drops PQ coverage
  precisely where signatures live longest.
- **Depending on the p2p crate from the license server** — drags libp2p, tokio
  networking, and the session machinery into a small axum binary; wrong direction of
  coupling.
- **Duplicating the envelope in `license_format`** — a security-sensitive fork that
  would drift from the audited implementation.
