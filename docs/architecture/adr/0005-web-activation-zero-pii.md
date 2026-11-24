# 0005 — Web-side activation with zero PII

## Status

Accepted (2026-08)

## Context

Payments run outside the app stores through a merchant of record (Paddle), and the
resulting license must bind to a Fyles identity
([0002](0002-license-bound-to-contact-id.md)). Two hard product constraints:

1. **The app has no purchase or redemption feature.** Activation happens on the web;
   the app only *exports* its public license identity (same spirit as the contact
   share, dedicated UI) and periodically asks the server "am I licensed?".
2. **Zero PII.** No emails, no names, ever. The only stored purchase linkage is
   `sha256(paddle_transaction_id)` — enough for refund-webhook auto-revocation and for
   support cases where the buyer proves purchase with their Paddle receipt, but not
   joinable against Paddle's records by browsing our data.

App-store compatibility (secondary, rules in flux mid-2026): Apple guideline 3.1.3(b)
"multiplatform services" permits unlocking features purchased on the developer's
website, provided the app does not steer users to the external purchase (US storefront
additionally allows external purchase links since the May 2025 Epic ruling; one
reading of 3.1.3(b) lets Apple demand an IAP twin). Re-verify before each submission.

## Decision

The user exports a **license identity** from the app — `FYLES-ID1:` +
base64url(ContactId + public keys) + a 4-byte checksum, no display name — and activates
on our page (`activation-site/`): after a Paddle-hosted checkout, the
`transaction.completed` webhook records only `sha256(txn_id)` in `purchases`; the success
page returns with `?_ptxn=<txn id>` and sends the buyer to
`/activate?transaction_id=<id>`, where they paste the license identity. The server hashes
the txn id, matches the purchase, and binds it.

**The identity is never sent to Paddle.** The buyer submits identity *and* transaction id
to **us**, so no third party ever holds both halves: we hold `identity ↔ sha256(txn)`,
Paddle holds `payment-PII ↔ txn`, and neither alone links payment to identity. Carrying the
identity to Paddle as *custom data* to auto-bind from the webhook would hand the processor
exactly that correlation — the one this ADR exists to prevent.

The transaction id is a **weak bearer credential** — not high-entropy-secret, and it
appears on the buyer's receipt — but Paddle txn ids are high-entropy ULIDs (not
enumerable), so the only real risk is *leakage*, mitigated by binding first-come + no
rebind + fail-open. This is accepted, consistent with anti-tamper being an explicit
non-goal. There is likewise **no proof of possession** — anyone can bind a license to any
pasted identity (worst case: gifting pro). The real risk is a typo binding a paid license
to garbage, and the **checksum** on the identity is the guard (web form and server both
reject mangled input). Activation is idempotent for the same identity; a transaction bound
elsewhere is refused (rebinding is a manual support process).

Freshness after purchase ([0004](0004-fail-open-entitlement-evaluation.md)): devices
check on service start and weekly, and the app offers a **pulse** ("check license
now") that hits the server immediately — no waiting a week for a fresh subscription.

## Consequences

- Our systems never touch personal data; the privacy page only needs to cover the
  hashed transaction id (legal review as open item). Paddle never learns the identity.
- A lost transaction id is on the buyer's Paddle receipt — no code to lose, nothing to
  email.
- The app needs only three small rpcs (`GetLicenseIdentity`, `GetEntitlementStatus`,
  `TriggerLicenseCheck`) and no payment code whatsoever.
- The mobile apps stay passive about where to buy (store-compliance posture).

## Alternatives considered

- **In-app code redemption with signed requests** — proof of possession at activation,
  but adds a redemption feature and payment-adjacent UX to the app; rejected by design
  (the checksum covers the typo risk, and the keys-of-record check protects queries).
- **Email fulfillment** — rejected outright: stores PII.
- **Paddle-native license keys** — keeps the code ledger inside Paddle; our server
  must stay authoritative for binding and revocation.
