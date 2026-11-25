# Fyles — source-available release

> Send files without dependencies. No servers, no accounts, no cloud — devices talk directly
> to each other.

## What is Fyles?

Fyles is a cross-platform peer-to-peer file-sharing application. It recreates the convenience of
the "share a link and receive files" workflow you know from OneDrive / Google Drive / Dropbox, but
**without** any central service: there is no server, no account, and no third party that can see
your files. Devices establish direct, end-to-end encrypted connections to each other.

The distinguishing properties are:

- **Strong data security** — files never leave the users' devices. Nothing is stored on, or
  visible to, a provider.
- **No dependence** on a service that can fail, be shut down, or be surveilled.
- **Nothing to operate** — no server infrastructure to run.

> [!WARNING]
> **These properties hold in full only while Internet mode is off, which is the default.**
> Internet mode joins the public IPFS network, which third parties run, and to work at all it
> publishes metadata that anyone can read and record:
>
> - **Where your device is.** So that contacts can find it, the device publishes an entry in the
>   network's public distributed hash table: its device identifier with its current IP addresses
>   and ports. The identifier never changes. Anyone who knows it, or who watches the table, can
>   follow which addresses a device uses over time, and so roughly where it is and through which
>   provider.
> - **Whom you look for.** Finding a contact means asking the network's nodes for that contact's
>   device identifier. Those nodes, and anyone watching them, see which device asks for which.
> - **When you connect, and to whom.** Relay nodes that help two devices reach each other see both
>   devices' addresses and when they connect.
>
> File contents stay end-to-end encrypted, and no third party stores them. But Internet mode does
> depend on a public network and its bootstrap nodes, which can fail, be blocked or be surveilled.
> If metadata matters to you, keep Internet mode off.

It runs on **Android, Linux, Windows, iOS and macOS**. On a local network (e.g. home Wi-Fi) it
works reliably and crosses platform boundaries — much like AirDrop (Apple) or Quick Share
(Android), but between *any* of those platforms. Over the open internet, direct connectivity is
inherently less reliable (a limitation of how the internet is built). Optional relays are a
planned remedy that would not compromise the serverless design for local use.

Security is taken seriously: identities use **post-quantum** cryptography alongside classical
schemes, so transfers should remain confidential even against a hypothetical future quantum
attacker.

## What is *this* repository?

This is a **curated, source-available subset** of the Fyles codebase, published for
**transparency and code review** — so that interested (and especially security-minded) readers can
inspect how authentication, authorization and encryption are actually implemented.

**The source is meant to be read, not used as a product.** What you can use from here are the
prebuilt apps attached to the releases: for Linux through the flake, see
[Install on Linux with Nix](#install-on-linux-with-nix), and for Android as APKs, see
[Install on Android](#install-on-android).

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

### NixOS with Home Manager

This is the usual setup: each user configures Fyles in their own Home Manager configuration, and
the system opens their port in the firewall. A complete system flake, next to an existing
`configuration.nix`, looks like this:

```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    home-manager = {
      url = "github:nix-community/home-manager";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fyles.url = "github:MatthiasvB/fyles-release";
  };

  outputs = { nixpkgs, home-manager, fyles, ... }: {
    nixosConfigurations.my-computer = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ./configuration.nix
        home-manager.nixosModules.home-manager
        fyles.nixosModules.default
        {
          nixpkgs.config.allowUnfreePackages = [ "fyles" ];
          home-manager.useGlobalPkgs = true;

          home-manager.users.alice = {
            home.stateVersion = "26.05";
            programs.fyles = {
              enable = true;
              listenPort = 24001;
              openFirewall = true;
            };
          };
        }
      ];
    };
  };
}
```

What the parts do:
- `fyles.nixosModules.default` makes `programs.fyles` available in every Home Manager user's
  configuration, and lets the system open the ports those users ask for.
- `allowUnfreePackages` allows Fyles, which is not free software. `useGlobalPkgs` makes Home
  Manager use the system's packages, so that allowance covers every user. Without it, each user
  allows `fyles` in their own `nixpkgs.config`.
- `enable` installs the app and runs the Fyles daemon as alice's user service from login.
- `listenPort` pins the port other devices reach alice on. Leave it out and the app picks one, but
  then no firewall can know it.
- `openFirewall` opens that port over TCP and UDP, and mDNS (5353/udp) for finding devices nearby.
- `outputDir = "/home/alice/Fyles";` would put received files somewhere other than
  `~/Downloads/Fyles`.

`alice` must be a user in `configuration.nix`. Each further user gets a block of their own, with a
port of their own, since two users cannot share one. Apply it with
`sudo nixos-rebuild switch --flake .#my-computer`, then check after logging in:
`systemctl --user status fyles-daemon`. The app shows the pinned port as set by the system.

### NixOS without Home Manager

The same settings live in the system configuration, one entry per user:

```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fyles.url = "github:MatthiasvB/fyles-release";
  };

  outputs = { nixpkgs, fyles, ... }: {
    nixosConfigurations.my-computer = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ./configuration.nix
        fyles.nixosModules.default
        {
          nixpkgs.config.allowUnfreePackages = [ "fyles" ];

          programs.fyles.users = {
            alice = {
              listenPort = 24001;
              openFirewall = true;
            };
            bob = {
              listenPort = 24002;
              openFirewall = true;
              outputDir = "/home/bob/Fyles";
            };
          };
        }
      ];
    };
  };
}
```

Every user listed gets the app and the daemon as a user service. The options mean the same as
above.

### Home Manager on another distribution

```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    home-manager = {
      url = "github:nix-community/home-manager";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fyles.url = "github:MatthiasvB/fyles-release";
  };

  outputs = { nixpkgs, home-manager, fyles, ... }: {
    homeConfigurations.alice = home-manager.lib.homeManagerConfiguration {
      pkgs = import nixpkgs {
        system = "x86_64-linux";
        config.allowUnfreePackages = [ "fyles" ];
      };
      modules = [
        fyles.homeManagerModules.default
        {
          home.username = "alice";
          home.homeDirectory = "/home/alice";
          home.stateVersion = "26.05";
          programs.fyles = {
            enable = true;
            listenPort = 24001;
          };
        }
      ];
    };
  };
}
```

Apply it with `home-manager switch --flake .#alice`. Home Manager cannot change a system firewall,
so `openFirewall` has no effect here. Open the port yourself, with `ufw` for example:

```sh
sudo ufw allow 24001
sudo ufw allow 5353/udp
```

### Notes

- `inputs.nixpkgs.follows` works with a nixpkgs at least as new as the one in this repository's
  `flake.lock`, because the binaries need that glibc or newer. On an older nixpkgs, keep the
  flake's own, at the cost of a second copy of GTK in the store.
- Outside NixOS, a Nix-installed GUI app may need [nixGL](https://github.com/nix-community/nixGL)
  to find the system's OpenGL driver.

## Install on Android

Each [release](https://github.com/MatthiasvB/fyles-release/releases) carries four APKs. Download
one onto the phone, open it, and allow the install when Android asks.

> **Not sure which one? Take `…-android-universal.apk`.** It runs on every Android device that Fyles
> supports. The others are a third of its size, which saves data if you know your device:
>
> | APK | Size | For |
> |---|---|---|
> | `…-android-arm64-v8a.apk` | about 40 MB | Nearly every phone and tablet from the last years. **Most likely yours.** |
> | `…-android-armeabi-v7a.apk` | about 35 MB | Older or low-cost devices with a 32-bit system. |
> | `…-android-x86_64.apk` | about 40 MB | Emulators and some Chromebooks. |
> | `…-android-universal.apk` | about 105 MB | All of the above. |
>
> If you pick one that does not fit your device, Android refuses with "App not installed", and
> nothing on the phone changes. Then take the universal one.

All four are the same app with the same version, so a later release updates whichever one you
installed, and you can switch between them at any time.

**Coming from Google Play?** A Play install may be signed with a different key than these APKs. If
it is, Android refuses to install one over the other. To switch, make a backup in the app first,
uninstall, install the other, and restore the backup.

**To verify a download,** compare `sha256sum` of the file with the digest GitHub shows beside
it, and check who signed it:

```sh
apksigner verify --print-certs fyles-*-android-arm64-v8a.apk
```

The signing certificate's SHA-256 must be
`52a65ec91e09079994a0229775a368ecb2b31c59f03c24b6f745e9ace8055aa3`.

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
networking, file I/O, the API server — as Rust traits. Concrete adapters (`p2p/`,
`direct_host_utils/`, the SQLite layer, …) are injected at startup via factory closures. The same
core can therefore run as a desktop daemon, an Android service, or an iOS background task. The
domain core, `Brain`, is an **actor**: it owns all state and runs a `tokio::select!` loop consuming
typed command messages over an `mpsc` channel — no shared mutable state, no locks in the hot path.

Files are transferred in **chunks** over libp2p — resumably, and **encrypted per session**
(ChaCha20-Poly1305 with keys from a hybrid X25519 + Kyber exchange). Authorization is by
cryptographic identity: when the first chunk of a transfer arrives, the receiver verifies 
that the sender is authorized for that particular filerequest before accepting any data.

Users don't manage device lists. All devices belonging to one user share the same cryptographic
material (`ContactKeys`, signing with Ed25519 and Dilithium5). Because they sign with the same keys, the
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

The Fyles app itself, including the prebuilt apps attached to this repository's releases, is
governed by the [Terms of Use](https://fyles.app/en/terms).

`fyles_service/zxcvbn_lite/` holds third-party code under the MIT licence, which its own `LICENSE`
file grants.
