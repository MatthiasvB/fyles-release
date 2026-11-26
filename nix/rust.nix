{ pkgs }: rec {
  toolchain = pkgs.rust-bin.fromRustupToolchainFile ../fyles_service/rust-toolchain.toml;
  nativeBuildInputs = with pkgs; [
    grpc-tools
    sqlite
    pkg-config
  ];
  buildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [ pkgs.libiconv ];
  shellPackages = [ toolchain ] ++ nativeBuildInputs ++ buildInputs;
}
