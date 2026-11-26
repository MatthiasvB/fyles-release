{ pkgs, lib, ... }:
let
  pubspec = ./fyles/pubspec.yaml;
  # The export to fyles-release leaves the app out.
  hasApp = builtins.pathExists pubspec;
  # The sandboxed check has no .dart_tool to read the package's language version from, and dart
  # format would fall back to the newest style.
  dartLanguageVersion = builtins.head (
    builtins.match ".*sdk: .>=([0-9]+\\.[0-9]+)\\..*" (builtins.readFile pubspec)
  );
in
{
  projectRootFile = "flake.nix";
  programs = {
    nixfmt.enable = true;
    rustfmt.enable = true;
    dart-format = lib.mkIf hasApp {
      enable = true;
      # Formatter output changes between SDK releases, so it uses the dart the dev shell's Flutter
      # brings.
      package = pkgs.flutter.dart;
    };
  };
  settings.formatter = lib.mkIf hasApp {
    dart-format.options = [
      "--language-version"
      dartLanguageVersion
    ];
  };
}
