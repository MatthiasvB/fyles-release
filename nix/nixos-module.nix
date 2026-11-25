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
      openFirewall = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Opens this user's listenPort over TCP and UDP in the firewall. It also opens mDNS (5353/udp),
          which devices nearby need to find this one. mDNS is one port for the whole system, so it is
          open while any Fyles user sets openFirewall, and closed when none does.
        '';
      };
    };
  };

  homeManagerUsers =
    lib.optionalAttrs hasHomeManager
    (lib.filterAttrs (_: user: user.programs.fyles.enable) config.home-manager.users);

  # Every Fyles user's settings, whether from programs.fyles.users or from Home Manager.
  fylesUsers =
    cfg.users
    // lib.mapAttrs (_: user: user.programs.fyles) homeManagerUsers;
  ports = lib.filter (port: port != null) (lib.mapAttrsToList (_: user: user.listenPort) fylesUsers);
  openPorts = lib.mapAttrsToList (_: user: user.listenPort) (lib.filterAttrs (_: user: user.openFirewall) fylesUsers);

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
          ExecStartPre = "${cfg.package}/libexec/fyles/wait-for-dns";
          ExecStart = startForUser;
          Restart = "on-failure";
          RestartSec = 5;
          SuccessExitStatus = toString cfg.package.passthru.exitAlreadyRunning;
        };
      };
    })

    (lib.mkIf (openPorts != []) {
      networking.firewall.allowedUDPPorts = [5353] ++ openPorts;
      networking.firewall.allowedTCPPorts = openPorts;
    })

    {
      assertions =
        [
          {
            assertion = lib.allUnique ports;
            message = "Two Fyles users pin the same listen port.";
          }
          {
            assertion = lib.intersectLists (lib.attrNames cfg.users) (lib.attrNames homeManagerUsers) == [];
            message = "Configure a Fyles user either in programs.fyles.users or in Home Manager, not both.";
          }
        ]
        ++ lib.mapAttrsToList (name: user: {
          assertion = user.openFirewall -> user.listenPort != null;
          message = "Fyles user ${name} sets openFirewall without a listenPort, but only a pinned port can be opened.";
        })
        fylesUsers;
    }
  ];
}
