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

**The source is meant to be read, not used as a product.** The one way to use Fyles from here is
the prebuilt Linux app the flake offers, see [Install on Linux with Nix](#install-on-linux-with-nix).

- It contains only the Rust backend crates that the `fyles-daemon` binary depends on. The
  **Flutter frontend** (the actual UI), the **internet-transport crates** (`*_internet`), and the
  **mobile/desktop FFI bindings** are deliberately **excluded** — so even though it compiles, it is
  not a usable application on its own.
- It is a **one-way export** from a private monorepo, which remains the source of truth. This repo
  is not the development workflow — issues and pull requests here are not actively used.

For the licence, see [Licence](#licence): source-available, **not** open source.

## Install on Linux with Nix

The flake offers the Linux app prebuilt, for x86_64. It is built from the private monorepo and
patched against this flake's nixpkgs, so it runs on NixOS and on any Linux with Nix.

Fyles is not free software, so Nix installs it only once you allow that:

```sh
NIXPKGS_ALLOW_UNFREE=1 nix run --impure github:MatthiasvB/fyles-release            # try it
NIXPKGS_ALLOW_UNFREE=1 nix profile install --impure github:MatthiasvB/fyles-release  # install it
```

`fyles` starts the Fyles daemon in the background, then opens the app. Files only arrive while the
daemon runs. To keep it running from login, enable the user unit the package ships:

```sh
systemctl --user link ~/.nix-profile/lib/systemd/user/fyles-daemon.service
systemctl --user enable --now fyles-daemon
```

Received files go to `~/Downloads/Fyles`. Other devices reach yours on a port the app picks, which
a firewall must let in, together with mDNS (5353/udp).

### NixOS

Add the input and the module to the system flake, and allow Fyles in the configuration's unfree
policy:

```nix
inputs.fyles.url = "github:MatthiasvB/fyles-release";
# in nixosSystem's modules:
fyles.nixosModules.default
# in the configuration:
nixpkgs.config.allowUnfreePackages = [ "fyles" ];
```

With Home Manager inside NixOS, each user configures Fyles in their own Home Manager config, and
the module opens the firewall for every pinned port:

```nix
home-manager.users.alice.programs.fyles = {
  enable = true;
  listenPort = 24001;
};
home-manager.users.bob.programs.fyles = {
  enable = true;
  listenPort = 24002;
  outputDir = "/home/bob/Fyles";
};
```

With `home-manager.useGlobalPkgs`, the system's unfree policy covers these users. Without it, each
of them allows `fyles` in their own `nixpkgs.config`.

Without Home Manager, the same settings live in the NixOS config:

```nix
programs.fyles.users = {
  alice.listenPort = 24001;
  bob = {
    listenPort = 24002;
    outputDir = "/home/bob/Fyles";
  };
};
```

Either way every listed user gets the daemon as a user service (`systemctl --user status
fyles-daemon`), and the app shows the pinned port as set by the system.

### Home Manager on another distribution

```nix
imports = [ fyles.homeManagerModules.default ];
nixpkgs.config.allowUnfreePackages = [ "fyles" ];
programs.fyles = {
  enable = true;
  listenPort = 24001;
};
```

With `home-manager.useGlobalPkgs`, the system configuration's unfree policy applies instead.

Home Manager cannot change a system firewall, so let that port in over TCP and UDP, and 5353/udp.

### Notes

- `inputs.nixpkgs.follows` works with a nixpkgs at least as new as the one in this repository's
  `flake.lock`, because the binaries need that glibc or newer. On an older nixpkgs, keep the
  flake's own, at the cost of a second copy of GTK in the store.
- Outside NixOS, a Nix-installed GUI app may need [nixGL](https://github.com/nix-community/nixGL)
  to find the system's OpenGL driver.

## What's in here

```
wire/                       # protobuf definitions for the on-the-wire / gRPC types
fyles_service/
  crypto/                   # classical + post-quantum primitives, session keys, signatures
  core/                     # domain core: the Brain actor, ports (traits), SQLite, wire/gRPC
  p2p/                      # local-network P2P adapter built on libp2p
  direct_host_utils/        # desktop host integration (filesystem, notifications, i18n)
  license_format/           # pro-licence certificate format and the trusted keys of record
  daemon/                   # the fyles-daemon binary that wires it all together
docs/                       # architecture documentation — start at docs/ARCHITECTURE.md
flake.nix                   # nix dev shell pinning the exact Rust toolchain (`nix develop`),
                            #   plus the Linux app package and its NixOS / Home Manager modules
nix/                        # the toolchain pin, the package and the modules
```

The `libp2p` fork this workspace patches in is pinned by an `ssh://` URL, so `cargo build`
needs a GitHub account with an SSH key — or that URL rewritten to `https://`.

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

The one exception is the Nix packaging, `flake.nix` and the files under `nix/`. You may use it to
install and run Fyles as described in [Install on Linux with Nix](#install-on-linux-with-nix).

The Fyles app itself, including the prebuilt Linux binaries attached to this repository's releases,
is governed by the [Terms of Use](https://fyles.app/en/terms).

`fyles_service/zxcvbn_lite/` holds third-party code under the MIT licence, which its own `LICENSE`
file grants.
