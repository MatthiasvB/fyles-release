# Why the Nix packaging looks the way it does

The decision to ship Linux as prebuilt binaries patched by this flake is
[ADR 0013](../docs/architecture/adr/0013-linux-through-nix.md). This page explains the places where
the Nix code departs from what the Nix community usually does, what each departure costs, and what
would let it go.

## The app is patched against this flake's nixpkgs

A prebuilt package usually takes its libraries from whatever nixpkgs the user has, and
`inputs.fyles.inputs.nixpkgs.follows = "nixpkgs"` would make that the case here too. It works for
packages whose vendors build against an old glibc, so that any nixpkgs is new enough.

Fyles' binaries are built with this repository's `flake.lock`, which tracks nixos-unstable, so they
need that glibc and libstdc++ or newer. glibc runs binaries built against older versions, never
newer ones. Patched against an older nixpkgs, the package still builds, and then fails at start with
`version 'GLIBC_…' not found`. Patching against the nixpkgs the build used is the one choice that
always fits, because the export copies the same `flake.lock`.

**Cost:** when the user's nixpkgs differs, the store holds a second GTK stack, a few hundred MB.

**What would let it go:** binaries without the high floor, which is a far-future idea in the
TechnicalTodo and not planned. The daemon could link statically against musl, and the app's native
parts would need an older base. Then the modules could use the user's `pkgs` like any other
package.

## The modules import nixpkgs again (`fylesFor` in `flake.nix`)

The rule in the community is that a module builds its package from the `pkgs` it is given, so that
the user's overlays and configuration apply. These modules instead import this flake's nixpkgs once
more, for the reason above, and pass on only the user's unfree settings: `allowUnfree`,
`allowUnfreePackages` and `allowUnfreePredicate`.

A simpler version set `allowUnfree` in the flake's own nixpkgs. That installed Fyles for users who
had decided against unfree software, so the user's policy is now carried over instead.

**Costs:**
- Every configuration that uses a module evaluates one more nixpkgs, which takes time and memory.
- The user's overlays and other nixpkgs settings do not reach Fyles. Only the unfree policy does.
- `nix run` and `nix profile install` have no configuration to take a policy from, so they need
  `NIXPKGS_ALLOW_UNFREE=1` with `--impure`, as every unfree package in nixpkgs does.

**What would let it go:** the same as above. With portable binaries, `package` would default to
`pkgs.callPackage ./fyles-app.nix {}`.

## One NixOS user unit for every user

`systemd.user.services` in NixOS installs a single unit that every user's systemd runs, including
the login screen's. NixOS has no per-user variant. The unit therefore runs a script that looks up
`$USER` in `programs.fyles.users`, exports that user's settings and starts the daemon, and exits
cleanly for anyone not listed.

## Home Manager inside NixOS

The NixOS module adds the Home Manager module to every user (`home-manager.sharedModules`) and
reads their settings back to open the firewall, which Home Manager cannot do itself. Three details
follow from that:
- The injection is guarded by `options ? home-manager`. `mkIf` would still name the
  `home-manager` option, which is an error where Home Manager is absent. A condition on `config`
  would recurse, so it asks the declared options instead.
- The Home Manager module carries a `key`. A user who also imports it by hand would otherwise
  declare every option twice, because two imports of a function value look like two modules.
- Home Manager running on its own is invisible to NixOS. Such users configure Fyles in
  `programs.fyles.users` instead. A unit Home Manager installs would replace the system's, since
  systemd prefers `~/.config/systemd/user`.

## Values kept in step by hand

- The exit code 75 for "another daemon already runs" lives in Rust (`EXIT_ALREADY_RUNNING`) and in
  `fyles-app.nix` (`exitAlreadyRunning`), and reaches the units through `passthru`.
- The desktop file is named after the app's ID, `app.fyles.fyles`, which the Flutter app sets in
  its Linux build. Wayland compositors find a window's icon through that name.
