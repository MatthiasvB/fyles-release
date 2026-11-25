{fylesFor}: {
  config,
  lib,
  pkgs,
  ...
}: let
  cfg = config.programs.fyles;
in {
  # Home Manager inside NixOS also gets this module from the NixOS module. The key loads it once.
  key = "fyles-home-manager-module";

  options.programs.fyles = {
    enable = lib.mkEnableOption "Fyles and its daemon as a user service";
    package = lib.mkOption {
      type = lib.types.package;
      default = fylesFor pkgs;
      defaultText = lib.literalMD "the flake's package, allowed by this configuration's unfree policy";
    };
    outputDir = lib.mkOption {
      type = lib.types.nullOr (lib.types.strMatching "/.*");
      default = null;
      description = "The absolute path received files go to. Unset means ~/Downloads/Fyles.";
    };
    listenPort = lib.mkOption {
      type = lib.types.nullOr (lib.types.ints.between 1024 65535);
      default = null;
      description = ''
        Pins the port other devices reach this user's node on, over TCP and UDP. Unset leaves it
        to the app. A firewall must let it in, see openFirewall.
      '';
    };
    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = ''
        Opens listenPort over TCP and UDP in the NixOS firewall. It also opens mDNS (5353/udp), which
        devices nearby need to find this one. mDNS is one port for the whole system, so it is open
        while any Fyles user sets openFirewall, and closed when none does. Only takes effect when
        Home Manager runs inside NixOS with the Fyles NixOS module imported. Elsewhere, Home Manager
        cannot change a firewall.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    home.packages = [cfg.package];
    systemd.user.services.fyles-daemon = {
      Unit.Description = "Fyles daemon";
      Service = {
        ExecStartPre = "${cfg.package}/libexec/fyles/wait-for-dns";
        ExecStart = lib.getExe' cfg.package "fyles-daemon-start";
        Restart = "on-failure";
        RestartSec = 5;
        SuccessExitStatus = toString cfg.package.passthru.exitAlreadyRunning;
        Environment =
          lib.optional (cfg.outputDir != null) "FYLES_OUTPUT_DIR=${cfg.outputDir}"
          ++ lib.optional (cfg.listenPort != null) "FYLES_LISTEN_PORT=${toString cfg.listenPort}";
      };
      Install.WantedBy = ["default.target"];
    };
  };
}
