# 0004 — Fail-open entitlement evaluation

## Status

Accepted (2026-08)

## Context

The product decision: if the license server is unreachable, clients default to **pro**
behavior. A dead or discontinued server must never degrade the app. Tamper-resistance
is a non-goal — a user who blocks the license domain gets pro, and that is accepted.

Naive fail-open is leakier than intended, though: if "server unreachable → pro", then a
free user can go offline after one check and be pro on every subsequent start. And
freshness matters in the other direction too: a purchase or new subscription should
take effect in minutes, not after a weekly timer.

## Decision

Entitlement evaluation (`core/src/core/license/evaluate.rs`) is a pure function of the
cached state, the *current* identity's ContactId, and the clock:

1. **Verified certificate wins.** If the cached cert verifies against a trusted server
   key and names the current ContactId ([0002](0002-license-bound-to-contact-id.md)):
   entitlements = the cert's unexpired entries. Authoritative regardless of network.
2. **Fresh definitive "no" means free.** If the server answered `NoLicense` within
   `LICENSE_FREE_ANSWER_TTL` (7 days), the app is free-tier. This negative cache is
   what keeps fail-open from being trivially farmable.
3. **Otherwise: provisional pro.** No verified cert, no fresh negative — grant pro,
   flagged `provisional`, and keep retrying in the background.

Re-check cadence: a 6-hour tick that only contacts the server when due —
`LICENSE_STARTUP_MIN_INTERVAL` (1 h) on the first tick after service start,
`LICENSE_RECHECK_INTERVAL` (7 d) thereafter — plus the **pulse** rpc
(`TriggerLicenseCheck`) for an immediate check on demand (fresh purchase, new
subscription). Every `UpdateIdentity` re-evaluates instantly and pushes
`EntitlementsChanged` to attached clients.

## Consequences

- A licensed user is never blocked by server downtime; an unlicensed user gets at most
  `LICENSE_FREE_ANSWER_TTL` of free-tier memory before fail-open resumes.
- The `provisional` flag lets the UI communicate "pro, pending verification" honestly
  instead of silently flip-flopping.
- The free-answer TTL is a product lever: shorter = leakier fail-open, longer = stickier
  free tier. 7 days is the opening bid.
- Subscriptions need no extra client machinery: their expiry (period end + grace) is in
  the signed certificate, and refreshes re-sign it.

## Alternatives considered

- **Pure fail-open without the negative cache** — one successful "free" answer followed
  by airplane mode yields permanent pro.
- **Fail-closed with a grace window** — rejected by the product decision; a discontinued
  server must not brick pro features.
- **Client-side staleness degradation of entitlements** — redundant with signed expiry
  dates and adds a second clock to reason about.
