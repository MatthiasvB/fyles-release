# 0006 — License server placement, transport, replay protection, and operations

## Status

Accepted (2026-08)

## Context

The license server ([0002](0002-license-bound-to-contact-id.md)–
[0005](0005-web-activation-zero-pii.md)) must reuse the `crypto` crate's identity
scheme, be **hostable now** (pre-monetization) by a single operator, and stay cheap —
fail-open makes downtime tolerable, so one small instance suffices. The system gains
its first-ever call to a central server — which must not add an HTTP/TLS stack to the
Rust core, since that core cross-compiles through the mobile FFI builds (`c_ffi` for
iOS, `kotlin_ffi` for Android) where `reqwest`+`rustls` do not travel cleanly.

## Decision

**Placement.** `license_format` (shared certificate/protocol types + verification) and
`license_server` (axum binary) are members of the `fyles_service` cargo workspace:
shared lockfile, shared flake/direnv toolchain, plain path dependency on `crypto`.
The Svelte activation SPA lives in top-level `activation-site/` (`public-site/` stays
no-build-step). The server serves the built SPA itself (axum `ServeDir` fallback), so
the SPA's endpoints are same-origin.

**Activation & admin.** A license is created only by activation (transaction id) or
post-disaster recovery. `--allow-test-activation` (dev/test
only, never in prod) lets `/activate` accept the magic transaction id `ACTIVATE-FOR-TEST`
so the flow works without a real purchase. Administration: `license_server admin
grant|revoke|unrevoke|deny|undeny|list` operating directly on the DB file, plus raw
sqlite3.

**Transport — the frontend relays, the core signs and verifies.** Each signed message is
a protobuf payload wrapped in `crypto::Signed<Vec<u8>>` (the signature covers the raw
bytes, so an older client verifies and decodes the fields it knows, skipping any a newer
server added), sent as `application/octet-stream` on `/v1/…`; JSON for the SPA's
`/v1/activate`. The Rust core does **not** open the socket, nor does it know the server
URL: it builds and signs the `LicenseQuery`, hands the opaque bytes to the frontend
(`PrepareLicenseCheck`), the frontend POSTs them to the URL it owns with its own HTTP
stack, and returns the response bytes (`SubmitLicenseCheckResponse`) for the core to
verify against the
trusted server keys and cache. This mirrors the existing opaque-relay idiom (`Settings`
/ `SettingsRequestEnvelope`). Rationale: `reqwest`+`rustls` do not cross-compile
cleanly through the mobile FFI builds (the aws-lc-rs/ring cmake/NASM trap), and there
is no reason to fight it — TLS is transport privacy only, while message authenticity is
the dual-signed certificate the core alone can verify. A hostile frontend can at most
withhold pro (self-harm); it can never forge a cert. The core links no HTTP client at
all. Cadence *policy* stays in the core (it owns the cache + the `LICENSE_*` intervals):
`PrepareLicenseCheck` returns "not due" unless the interval is up or `force` (a pulse)
is set; a never-checked install is always due.

**No replay layer.** The signed requests carry no timestamp or nonce, and there is no
`seen_nonces` table. It would be pure ceremony: `/v1/license/current` is idempotent (a
replay only ever returns the holder's own entitlement), and `/v1/keys/update`
succession must be signed by the *current* keys-of-record, so a stale update cannot
roll the authenticators back. A version field is the only guard.

**Backup & recovery.** Nightly `VACUUM INTO` snapshots, age-encrypted, shipped
off-host (`ops/backup.sh`). Independently, the query protocol carries the client's
cached certificate: after a server data loss, a valid previously-issued cert
re-creates its license row — including the hashed vendor transaction id, which is
**carried inside the signed certificate** for exactly this reason. Active licensees
therefore self-heal across restore gaps. Signing key: generated offline by
`license_server keygen` (seeds only, 0600), stored separately from the DB, encrypted
at rest; its public half ships in clients' append-only trusted-key list
(`signing_key_id` selects it). Rotation is therefore a **rollout, not a flip**: ship a
client release trusting key N+1, wait for adoption, then start the server with
`--signing-key-id N+1`. Un-updated clients can't verify the new certs and fall back to
*provisional* pro by fail-open — payers keep pro, enforcement merely lapses for stale
installs until they update, which the threat model already tolerates.

**Deployment: nix `dockerTools`.** The image is a flake output
(`packages.license-server-image`, `dockerTools.buildLayeredImage`) built from the
**unified project flake** at `fyles/flake.nix` — which spans `fyles_service/`, `wire/`,
and `activation-site/`, so one flake builds the server (crane, `-p license_server`) and
the SPA (`buildNpmPackage`) and layers them together. dockerTools bundles the
`/nix/store` closure into the image, so `/nix/store` linking is a feature, not a
blocker. The libp2p patch is a *public* fork whose checked-in URL is `ssh://` only for
the maintainer's push workflow; the build rewrites it to `git+https://` (a
`substituteInPlace` on `Cargo.toml`+`Cargo.lock` in the assembled source) so the
sandbox fetches it without an ssh agent — verified: nix vendors
`cargo-git-https-…rust-libp2p`. Because `build.rs` compiles the sibling `wire/` protos
(`../../wire`), the build assembles a tree with `wire/` inside the workspace and
repoints those paths. TLS terminates at a reverse proxy
(`ops/docker-compose.example.yml`); the SPA is baked in and served same-origin.
Resulting image: ~52 MB.

## Consequences

- One toolchain, one lockfile, one `cargo test --workspace`. A license exists only after
  activation or recovery; `--allow-test-activation` covers pre-monetization/dev testing.
- The recovery path implies: whoever holds a cached cert *and* usable keys after a
  server wipe can register those keys as keys-of-record. A stolen cert blob enables a
  narrow hijack window post-disaster — accepted, ≤ fail-open in impact.
- `export-public.sh` scope: exclude `license_server` (operational), include
  `license_format` (a `core` dependency).
- Server downtime also takes down the buy/activate pages — acceptable; Paddle's
  checkout itself is Paddle-hosted and fail-open covers existing users.

## Alternatives considered

- **Separate cargo workspace** — forks lockfile and toolchain for no isolation gain.
- **TypeScript/Kotlin server with the crypto crate behind FFI** — FFI build, packaging,
  and byte-identical serialization across the boundary cost more than the small axum
  server they would make marginally easier to write.
- **JSON for the service protocol** — would need hand-written codecs for binary key and
  signature types. The payloads are protobuf (forward-compatible field skipping) inside a
  `Signed<Vec<u8>>` envelope that stays bincode (a stable, fixed schema).
- **An HTTP client in the Rust core** (`reqwest`+`rustls`) — the obvious shape, but it
  drags aws-lc-rs/ring into the mobile FFI cross-compile; relaying the socket through
  the frontend sidesteps it entirely and keeps verification where the trust is.
- **An in-Docker multi-stage build** — a hand-rolled Dockerfile duplicating the toolchain.
  The nix `dockerTools` image reuses the flake's one toolchain instead (the libp2p fork is
  public, so ssh→https fetches it and dockerTools bundles the store closure).
- **Litestream continuous replication** — near-zero loss window but one more moving
  part; snapshots + cert-based self-healing cover the actual risk here.
