# Pro Licensing

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)
> **Decisions:** [ADR 0003](adr/0003-hybrid-signatures-for-license-certificates.md) ·
> [0004](adr/0004-fail-open-entitlement-evaluation.md) ·
> [0005](adr/0005-web-activation-zero-pii.md) ·
> [0006](adr/0006-license-server-placement-transport-replay.md) ·
> [0009](adr/0009-contact-id-derived-from-identity-keys.md)
> (superseding [0002](adr/0002-license-bound-to-contact-id.md))

Pro mode is enabled by a **server-signed license certificate bound to the identity's
`ContactId`**, cached locally and evaluated offline. Feature restrictions themselves
are enforced in the clients (mechanics TBD); this subsystem only answers *"which
entitlements does the current identity hold?"*.

Design invariants:

- **Fail-open**: an unreachable (or discontinued) license server defaults clients to
  pro. Anti-tamper is a non-goal.
- **Zero PII**: no emails, no names, ever. The only purchase linkage stored is
  `sha256(paddle_transaction_id)`.
- **ContactId-bound, key-authenticated**: one purchase upgrades every device sharing
  the identity (devices clone `ContactKeys`, and the ContactId follows from them, see
  [security.md](security.md) §7.2). The ContactId *is* `SHA-256` of the identity's public
  keys, so a key set either owns the id it claims or it does not. The server keeps
  **keys-of-record** per ContactId and verifies every query's dual signature against them.
  Key rotation is unimplemented. When it arrives it will change the identity, and a
  succession statement signed by the old keys will move the license with it.
- **Entitlement set**: certs carry `{ id, expires_at?, vendor_txn_hash? }` entries — the
  hash is per entitlement, so distinct purchases stay individually linked and refundable.
  One-time `pro` never expires; future subscription extensions carry period-end + grace.
- **Dev/test**: `--allow-test-activation` lets `/activate` accept the magic transaction id
  `ACTIVATE-FOR-TEST` so the flow can be exercised without a real purchase. Never in prod.

---

## 1. Components

| Component | Location | Role |
|-----------|----------|------|
| `crypto::signed` | `fyles_service/crypto/src/signed.rs` | Generic dual-signature envelope `Signed<P>` (Ed25519 + ML-DSA-87), promoted from the p2p crate |
| `license_format` | `fyles_service/license_format/` | Certificate + protocol types, `PublicLicenseIdentity` text codec (checksummed), trusted server keys |
| `license_server` | `fyles_service/license_server/` | axum binary: license queries, activation, Paddle webhook, admin CLI; serves the SPA. See its `README.md` for ops |
| `core::license` | `fyles_service/core/src/core/license.rs` | Client side: SQLite cache, pure evaluation, cadence gate; signs the query + verifies the answer, but the HTTP is relayed through the frontend (no HTTP client in the core) |
| Activation site | `activation-site/` | Vite + Svelte + TS SPA: landing/checkout, success page (shows the transaction id), `/activate` form (identity + transaction id) with live checksum validation |

## 2. Certificate & license identity

```rust
LicenseCertificatePayload {
    version,
    contact_id,           // the binding, derived from the identity's public keys
    entitlements,         // [{ id: "pro", expires_at: None, vendor_txn_hash: Some(…) }, …]
    issued_at, serial,
}
// Encoded as protobuf, wrapped in crypto::signed::Signed<Vec<u8>> and dual-signed by the
// server's ContactKeys. The envelope's `signed_by` names the server key's trusted-list
// index (append-only → rotation). Protobuf-over-opaque-bytes lets an older client verify
// (the signature covers the raw bytes) and decode the fields it knows, skipping any a
// newer server added.
```

The **license identity** the user exports from the app (`GetLicenseIdentity` rpc) is
`FYLES-ID2:` + base64url(bincode(public keys)) + 4-byte SHA-256 checksum — no ContactId
(it derives from the keys) and no display name. The checksum is the mistype guard standing in for
proof-of-possession at activation; the web form validates it live
(`activation-site/src/lib/identity.ts` mirrors `license_format::identity`).

## 3. Evaluation (client)

Pure function (`core/src/core/license/evaluate.rs`), in priority order:

1. Cached cert verifies against a trusted key **and** names the current ContactId →
   the cert's unexpired entitlements. Authoritative offline, and a restored or foreign
   identity instantly stops matching.
2. Server said `NoLicense` within `LICENSE_FREE_ANSWER_TTL` (7 d) → **free**.
3. Otherwise → **pro, `provisional: true`** (fail-open), retried in the background.

Re-check cadence: the frontend pokes `PrepareLicenseCheck`; the core gates it — a
never-checked install is due immediately, an established one waits
`LICENSE_RECHECK_INTERVAL` (7 d). A **pulse** (`PrepareLicenseCheck` with `force`, from
the "Check license now" button) bypasses the gate for an immediate check after a
purchase. When due, the core returns a signed request the frontend POSTs, then feeds the
answer back via `SubmitLicenseCheckResponse`. Changes are pushed as
`ServerMessage.entitlements_changed`; `UpdateIdentity` re-evaluates immediately.

## 4. Flows

**Purchase → activate (one path)**: Paddle-hosted checkout → the `transaction.completed`
webhook records only `sha256(txn_id)` in `purchases` (no identity, nothing else). The
success page returns with `?_ptxn=<txn id>` and sends the buyer to
`/activate?transaction_id=<id>`, where they paste their license identity. `/activate`
hashes the txn id, matches the purchase, and binds it to the identity — first-come TOFU,
idempotent for the same identity, and a transaction already bound elsewhere is refused
(no rebind). **The identity is never sent to Paddle**, so no third party holds a
`payment ↔ identity` link. A lost transaction id is on the Paddle receipt.

**License query** (`POST /v1/license/current`, a signed protobuf `LicenseQuery`): row
exists → verify against keys-of-record → fresh re-signed cert, or `NoLicense` if
revoked/denylisted. No row → verify against the payload keys, then: valid cached cert
→ **recovery** (row re-created from the cert, presenting keys become keys-of-record);
else `NoLicense`. (There is no auto-issue path; a license is created only by activation
or recovery.)

**Key rotation**: not implemented, and there is no `/v1/keys/update`. A rotation changes the
ContactId, so it will need a succession statement that moves the license rather than swapping
authenticators under a fixed id. A full identity loss is a support rebind via Paddle receipt.

**Refunds**: Paddle `adjustment.created` (refund/chargeback) → hash the transaction
id → revoke the matching license; the client sees `NoLicense` on its next check.

## 5. Server operations

See `fyles_service/license_server/README.md` for the runnable version: keygen,
`--allow-test-activation`, `admin grant|revoke|deny|list`, the OCI image (nix
`dockerTools`, built from the unified `fyles/flake.nix` as `packages.license-server-image`,
SPA baked in, TLS via reverse proxy), and `ops/backup.sh` (nightly `VACUUM INTO` + age)
with cert-based self-healing recovery on top. DB schema: `licenses` (keys-of-record),
`license_entitlements` (one row per entitlement, each with its own `vendor_txn_hash`), and
`purchases` (`sha256(txn_id)` only) — no activation-codes table (activation is by
transaction id) and no `seen_nonces` (no replay layer: the endpoints are idempotent).

## 6. Known accepted weaknesses

- No proof-of-possession at activation: worst case is gifting pro to a stranger;
  typos are caught by the checksum.
- Revocation is bounded by fail-open: blocking the server keeps provisional pro.
- Local cache tampering is out of scope by decision.

Two weaknesses closed by the key-derived ContactId
([ADR 0009](adr/0009-contact-id-derived-from-identity-keys.md)): squatting a licensed
ContactId is unrepresentable rather than merely refused, and post-disaster recovery can no
longer be driven by a *stolen cached cert* plus fresh keys, since those keys do not derive
the id the certificate names.

## 7. App-store note

Cross-platform unlock of a web purchase is permitted by Apple guideline 3.1.3(b)
("multiplatform services") as long as the app does not steer users to the external
purchase; since May 2025 the US storefront also allows external purchase links. One
reading of 3.1.3(b) lets Apple demand the unlock also be offered as IAP. Google Play
runs analogous regional alternative-billing programs. These rules shift — re-verify
before every store submission, and keep the mobile apps passive about where to buy.

## 8. Open items

1. Paddle account: validate webhook event shapes against the sandbox and confirm the
   success-page transaction id (`?_ptxn=`) equals the webhook's `data.id`; wire Paddle.js
   into the activation site's landing page.
2. Legal check that the hashed transaction id meets the zero-PII bar
   (`public-site/COMPLIANCE.md` / privacy page update before go-live).
3. `export-public.sh`: exclude `license_server`, include `license_format`.
