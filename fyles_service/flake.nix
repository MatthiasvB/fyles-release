{
  description = "rust-crane-template";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    crane = {
      url = "github:ipetkov/crane";
    };

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-utils.url = "github:numtide/flake-utils";
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };

  outputs = {
    self,
    nixpkgs,
    crane,
    flake-utils,
    treefmt-nix,
    rust-overlay,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {
          inherit system overlays;
          config.allowUnfree = true;
        };
          rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

        craneLib = (crane.mkLib pkgs).overrideToolchain (
          rustToolchain
        );

        fyles_be = craneLib.buildPackage {
          src = craneLib.cleanCargoSource (craneLib.path ./.);
          strictDeps = true;

          buildInputs = with pkgs;
            [
              # Add additional build inputs here
              grpc-tools
              sqlite
              # Java native bindings
              openjdk
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              # Additional darwin specific inputs can be set here
              pkgs.libiconv
            ];

          # Additional environment variables can be set directly
          # MY_CUSTOM_VAR = "some value";
        };

        treefmtEval = treefmt-nix.lib.evalModule pkgs ./treefmt.nix;
      in {
        formatter = treefmtEval.config.build.wrapper;

        checks = {
          inherit fyles_be;
          formatting = treefmtEval.config.build.check self;
        };

        packages.default = fyles_be;

        apps.default = flake-utils.lib.mkApp {
          drv = fyles_be;
        };

        devShells.android = pkgs.mkShell {
          buildInputs = [
            # Add additional build inputs here
            pkgs.rustup
            pkgs.cargo-ndk
            pkgs.grpc-tools
            pkgs.sqlite
          ];
        };

        # Lean shell for CI: the pinned toolchain plus the native libs needed to
        # compile/test the workspace, without the heavy rust-rover GUI that the
        # default dev shell pulls in. Used by `cargo test` and the Linux build.
        # Includes `dart` so the git-ignored Fluent .ftl translations (consumed
        # by direct_host_utils) can be regenerated from the committed master
        # before compiling — see fyles/tool/split_platform_translations.dart.
        devShells.ci = pkgs.mkShell {
          buildInputs =
            [
              rustToolchain
              pkgs.dart # regenerate git-ignored .ftl before compiling
              pkgs.grpc-tools # protoc for tonic-build (core, p2p_internet)
              pkgs.sqlite # system libsqlite3 for rusqlite
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.libiconv
            ];
        };

        devShells.default = craneLib.devShell {
          # Inherit inputs from checks.
          checks = self.checks.${system};

          # Additional dev-shell environment variables can be set directly
          # MY_CUSTOM_DEVELOPMENT_VAR = "something else";

          # This should not be needed but rust_analyzer is not picking up the
          # OUT_DIR environment variable.
          OUT_DIR = "out_dir";

          buildInputs = [
            pkgs.jetbrains.rust-rover
          ];

          # Extra inputs can be added here; cargo and rustc are provided by default.
          packages = [pkgs.rustup pkgs.cargo-ndk];


          shellHook = ''
            mkdir -p ~/.rust-rover/toolchain

            ln -sfn ${rustToolchain}/lib ~/.rust-rover/toolchain
            ln -sfn ${rustToolchain}/bin ~/.rust-rover/toolchain

            export RUST_SRC_PATH="$HOME/.rust-rover/toolchain/lib/rustlib/src/rust/library"
          '';
        };
      }
    );
}
