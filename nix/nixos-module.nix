{
  self,
  fylesFor,
}: {
  config,
  lib,
  pkgs,
  options,
  ...
}: let
  cfg = config.programs.fyles;
  hasHomeManager = options ? home-manager;

  userOptions = {
    options = {
      outputDir = lib.mkOption {
        type = lib.types.nullOr (lib.types.strMatching "/.*");
        default = null;
        description = "The absolute path this user's received files go to. Unset means ~/Downloads/Fyles.";
      };
      listenPort = lib.mkOption {
        type = lib.types.nullOr (lib.types.ints.between 1024 65535);
        default = null;
        description = "Pins the port other devices reach this user's node on. Unset leaves it to the app.";
      };
    };
  };

  homeManagerUsers =
    lib.optionalAttrs hasHomeManager
    (lib.filterAttrs (_: user: user.programs.fyles.enable) config.home-manager.users);

  ports = lib.filter (port: port != null) (
    lib.mapAttrsToList (_: user: user.listenPort) cfg.users
    ++ lib.mapAttrsToList (_: user: user.programs.fyles.listenPort) homeManagerUsers
  );

  # NixOS user units apply to every user, so the unit picks the logged-in user's settings.
  startForUser = pkgs.writeShellScript "fyles-daemon-for-user" ''
    case "$USER" in
    ${lib.concatStrings (lib.mapAttrsToList (name: user: ''
        ${lib.escapeShellArg name})
          ${lib.optionalString (user.listenPort != null)
          "export FYLES_LISTEN_PORT=${toString user.listenPort}"}
          ${lib.optionalString (user.outputDir != null)
          "export FYLES_OUTPUT_DIR=${lib.escapeShellArg user.outputDir}"}
          ;;
      '')
      cfg.users)}
      *) exit 0 ;;
    esac
    exec ${lib.getExe' cfg.package "fyles-daemon-start"}
  '';
in {
  options.programs.fyles = {
    package = lib.mkOption {
      type = lib.types.package;
      default = fylesFor pkgs;
      defaultText = lib.literalMD "the flake's package, allowed by this configuration's unfree policy";
    };
    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = "Opens mDNS (5353/udp) and every pinned listen port, over TCP and UDP.";
    };
    users = lib.mkOption {
      type = lib.types.attrsOf (lib.types.submodule userOptions);
      default = {};
      description = "Users who get the Fyles daemon as a user service, for setups without Home Manager.";
    };
  };

  config = lib.mkMerge [
    (lib.optionalAttrs hasHomeManager {
      home-manager.sharedModules = [self.homeManagerModules.default];
    })

    (lib.mkIf (cfg.users != {}) {
      environment.systemPackages = [cfg.package];
      systemd.user.services.fyles-daemon = {
        description = "Fyles daemon";
        wantedBy = ["default.target"];
        serviceConfig = {
          ExecStart = startForUser;
          Restart = "on-failure";
          SuccessExitStatus = toString cfg.package.passthru.exitAlreadyRunning;
        };
      };
    })

    (lib.mkIf (cfg.openFirewall && (cfg.users != {} || homeManagerUsers != {})) {
      networking.firewall.allowedUDPPorts = [5353] ++ ports;
      networking.firewall.allowedTCPPorts = ports;
    })

    {
      assertions = [
        {
          assertion = lib.allUnique ports;
          message = "Two Fyles users pin the same listen port.";
        }
        {
          assertion = lib.intersectLists (lib.attrNames cfg.users) (lib.attrNames homeManagerUsers) == [];
          message = "Configure a Fyles user either in programs.fyles.users or in Home Manager, not both.";
        }
      ];
    }
  ];
}
