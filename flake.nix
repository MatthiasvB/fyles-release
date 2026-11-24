{
  description = "Fyles — source-available subset";
  # This block must stay identical to flake.nix's: the export copies the monorepo flake.lock
  # verbatim, and a diverging input set would make nix rewrite that lock in the public repo.
  # `crane` is unused here for that reason alone. tools/export-public guards the invariant.
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix.url = "github:numtide/treefmt-nix";
  };
  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
    treefmt-nix,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [(import rust-overlay)];
      };

      rustEnv = import ./nix/rust.nix {inherit pkgs;};

      treefmtEval = treefmt-nix.lib.evalModule pkgs ./treefmt.nix;
    in {
      formatter = treefmtEval.config.build.wrapper;

      checks.formatting = treefmtEval.config.build.check self;

      packages = nixpkgs.lib.optionalAttrs (system == "x86_64-linux") rec {
        fyles = pkgs.callPackage ./nix/fyles-app.nix {};
        default = fyles;
      };

      devShells.default = pkgs.mkShell {
        packages = rustEnv.shellPackages;
        shellHook = ''
          echo "Using Rust from: $(command -v rustc)"
        '';
      };
    })
    // (let
      # The app is patched against this flake's nixpkgs, which its binaries were built with, but
      # only under the user's own unfree policy.
      fylesFor = userPkgs: let
        userConfig = userPkgs.config;
      in
        (import nixpkgs {
          inherit (userPkgs.stdenv.hostPlatform) system;
          config =
            {
              allowUnfree = userConfig.allowUnfree or false;
              allowUnfreePackages = userConfig.allowUnfreePackages or [];
            }
            // nixpkgs.lib.optionalAttrs (userConfig ? allowUnfreePredicate) {
              inherit (userConfig) allowUnfreePredicate;
            };
        }).callPackage
        ./nix/fyles-app.nix {};
    in {
      homeManagerModules.default = import ./nix/home-manager-module.nix {inherit fylesFor;};
      nixosModules.default = import ./nix/nixos-module.nix {inherit self fylesFor;};
    });
}
