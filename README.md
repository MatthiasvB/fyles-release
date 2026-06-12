# Fyles — source-available release

> Send files without dependencies. No servers, no accounts, no cloud — devices talk directly
> to each other.

## What is Fyles?

Fyles is a cross-platform peer-to-peer file-sharing application. It recreates the convenience of
the "share a link and receive files" workflow you know from OneDrive / Google Drive / Dropbox, but
**without** any central service: there is no server, no account, and no third party that can see
your data. Devices establish direct, end-to-end encrypted connections to each other.

The distinguishing properties are:

- **Strong data security** — files never leave the users' devices; nothing is stored on, or
  visible to, a provider.
- **No dependence** on a service that can fail, be shut down, or be surveilled.
- **Nothing to operate** — no server infrastructure to run.

It runs on **Android, Linux, Windows, iOS and macOS**. On a local network (e.g. home Wi-Fi) it
works reliably and crosses platform boundaries — much like AirDrop (Apple) or Quick Share
(Android), but between *any* of those platforms. Over the open internet, direct connectivity is
inherently less reliable (a limitation of how the internet is built); optional relays are a
planned remedy that would not compromise the serverless design for local use.

Security is taken seriously: identities use **post-quantum** cryptography alongside classical
schemes, so transfers should remain confidential even against a hypothetical future quantum
attacker.

## What is *this* repository?

This is a **curated, source-available subset** of the Fyles codebase, published for
**transparency and code review** — so that interested (and especially security-minded) readers can
inspect how authentication, authorization and encryption are actually implemented.

**It is meant to be read, not used as a product.**

- It contains only the Rust backend crates that the `fyles-daemon` binary depends on. The
  **Flutter frontend** (the actual UI), the **internet-transport crates** (`*_internet`), and the
  **mobile/desktop FFI bindings** are deliberately **excluded** — so even though it compiles, it is
  not a usable application on its own.
- It is a **one-way export** from a private monorepo, which remains the source of truth. This repo
  is not the development workflow — issues and pull requests here are not actively used.

For the licence, see [Licence](#licence): source-available, **not** open source.

## What's in here

```
wire/                       # protobuf definitions for the on-the-wire / gRPC types
fyles_service/
  crypto/                   # classical + post-quantum primitives, session keys, signatures
  core/                     # domain core: the Brain actor, ports (traits), SQLite, wire/gRPC
  p2p/                      # local-network P2P adapter built on libp2p
  direct_host_utils/        # desktop host integration (filesystem, notifications, i18n)
  daemon/                   # the fyles-daemon binary that wires it all together
docs/                       # architecture documentation — start at docs/ARCHITECTURE.md
```

## Architecture at a glance

The full architecture documentation lives in **[`docs/`](docs/ARCHITECTURE.md)** — start at
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), which links to focused sub-documents on the
[backend](docs/architecture/backend.md), [networking & P2P](docs/architecture/networking.md),
[security & cryptography](docs/architecture/security.md), and the
[wire protocol](docs/architecture/wire-protocol.md). A few highlights:

The system is **hexagonal** (ports & adapters). The `core` crate defines abstract ports — database,
networking, file I/O, the API server — as Rust traits; concrete adapters (`p2p/`,
`direct_host_utils/`, the SQLite layer, …) are injected at startup via factory closures. The same
core can therefore run as a desktop daemon, an Android service, or an iOS background task. The
domain core, `Brain`, is an **actor**: it owns all state and runs a `tokio::select!` loop consuming
typed command messages over an `mpsc` channel — no shared mutable state, no locks in the hot path.

Files are transferred in **chunks** over libp2p — resumably, and **encrypted per session**
(ChaCha20-Poly1305 with keys from a hybrid X25519 + Kyber exchange). Authorization is by
cryptographic identity: when the first chunk of a transfer arrives, the receiver verifies 
that the sender is authorized for that particular filerequest before accepting any data.

Users don't manage device lists. All devices belonging to one user share the same cryptographic
material (`ContactKeys`; Ed25519 + Dilithium5 signing). Because they sign with the same keys, the
network mathematically recognises them as the **same contact**.

> **Note:** the `docs/` tree describes the **complete** Fyles system — including the Flutter
> frontend and the internet transport that are *not* part of this repository (see the open-source
> strategy in `docs/ARCHITECTURE.md` §5.4). Treat references to crates or files you cannot find
> here as pointers into the parts that remain private.

## Licence

The code in this repository is **licensed as UNLICENSED** (see each crate's `Cargo.toml`). It is
published so that you can **read and review** it. It is **not** free / open-source software: no
rights to use, build, redistribute, or create derivative works are granted. All rights reserved.
