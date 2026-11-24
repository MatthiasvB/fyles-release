# 0002 — License bound to the ContactId, keys as rotatable authenticators

## Status

Accepted (2026-08). Superseded by [0009](0009-contact-id-derived-from-identity-keys.md) (2026-09).

## Context

Pro mode must upgrade *all* devices sharing a Fyles identity with one purchase, keep
working while the license server is unreachable (fail-open, see
[0004](0004-fail-open-entitlement-evaluation.md)), and stop applying the moment a
device's identity changes — e.g. after a backup restore, or when someone else
activated pro on a device before the owner re-installed their own identity.

The identity's key material is *not* a stable anchor: the post-quantum crates
(ml-dsa 0.1.x) may change encodings or be swapped entirely, and a future full key
rotation is conceivable. Binding the license to a key fingerprint would orphan every
license on any such change. The identity does have a durable handle, though: the
`ContactId`, cloned to every device together with the keys (`security.md` §7.2) and
independent of the key material.

## Decision

- **The license binds to the `ContactId`.** The certificate payload names it; the
  client evaluates a cached cert by comparing it against the *current*
  `SelfContact.id`.
- **The keys are authenticators, not the binding.** At activation the server stores
  the identity's public keys as **keys-of-record**; every license query is dual-signed
  (Ed25519 + ML-DSA-87) and verified **against the stored keys** — the server believes
  the asker actually holds the identity, and fresh keys cannot claim a licensed
  ContactId.
- **Key rotation is a key update, never a rebind**: a succession statement — the new
  key set, signed by the old keys — updates the keys-of-record (`/v1/keys/update`).
  Certificates are re-signed on every refresh, so rotated devices are re-certified on
  their next check-in.
- Certificates are offline-verifiable against server public keys compiled into the
  client, carry an **entitlement set** `{ id, expires_at? }` (one-time `pro` never
  expires; future subscriptions get period-end + grace), and include the hashed vendor
  transaction id so recovery restores the full record
  ([0006](0006-license-server-placement-transport-replay.md)).

The comparison-at-evaluation-time rule closes the identity-change hole with no
special-case code: a restored or foreign identity has a different ContactId, so the
cached certificate simply stops matching until a re-check succeeds under the new
identity.

## Consequences

- PQ-crate churn and even a full signature-scheme migration never orphan licenses —
  worst case is one succession round-trip per identity.
- The server stores public keys per license (they are public material; still no PII).
- A brand-new identity (old keys lost entirely) cannot run succession; that is a
  support case, provable via the Paddle receipt through the hashed transaction id.
- ContactIds are self-assigned UUIDs; squatting a licensed ContactId with different
  keys is rejected by keys-of-record verification. Injecting a foreign cert into the
  local cache is local tampering — out of scope (≤ fail-open in impact).

## Alternatives considered

- **Bind to a fingerprint of the full key set** — proof-carrying, but any ml-dsa
  encoding change or serde-layout drift orphans every license.
- **Bind to the Ed25519 key only** — stable format, but still breaks on an eventual
  Ed25519 rotation and leaves the PQ key outside the authentication story.
- **Per-device licensing** — fights the product model, where the identity is the unit.
- **Online entitlement checks per use** — contradicts the offline-first p2p design and
  the fail-open requirement.
