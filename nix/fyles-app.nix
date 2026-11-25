# The Linux app, packaged from the prebuilt tarball that fyles/release/linux.py produces. The source
# is not public, so the binaries are patched against this flake's nixpkgs rather than built here
# (docs/architecture/adr/0013-linux-through-nix.md).
{
  lib,
  stdenv,
  fetchurl,
  autoPatchelfHook,
  wrapGAppsHook3,
  makeWrapper,
  copyDesktopItems,
  makeDesktopItem,
  runtimeShell,
  coreutils,
  gnugrep,
  util-linux,
  xdg-user-dirs,
  gtk3,
  glib,
  pango,
  harfbuzz,
  atk,
  at-spi2-core,
  cairo,
  gdk-pixbuf,
  libepoxy,
  fontconfig,
  libX11,
  libGL,
  # Named so that callPackage, which fills every argument nixpkgs has an attribute for, leaves them.
  fylesRelease ? lib.importJSON ./fyles-release.json,
  fylesTarball ? fetchurl {inherit (fylesRelease) url hash;},
}: let
  # direct_host_utils::instance_lock::EXIT_ALREADY_RUNNING, which units count as a clean exit.
  exitAlreadyRunning = 75;
in
  stdenv.mkDerivation {
    pname = "fyles";
    inherit (fylesRelease) version;
    src = fylesTarball;

    nativeBuildInputs = [autoPatchelfHook wrapGAppsHook3 makeWrapper copyDesktopItems];
    buildInputs = [
      gtk3
      glib
      pango
      harfbuzz
      atk
      at-spi2-core
      cairo
      gdk-pixbuf
      libepoxy
      fontconfig
      libX11
      stdenv.cc.cc.lib
    ];
    # libepoxy opens the GL libraries at runtime, which autoPatchelf cannot see.
    runtimeDependencies = [libGL];
    # The bundle carries the `jni` package's library, which only Android opens.
    autoPatchelfIgnoreMissingDeps = ["libjvm.so"];
    dontWrapGApps = true;

    desktopItems = [
      (makeDesktopItem {
        # Wayland compositors find a window's icon through the desktop file named after its app
        # ID, which is APPLICATION_ID in fyles/linux/CMakeLists.txt.
        name = "app.fyles.fyles";
        startupWMClass = "app.fyles.fyles";
        desktopName = "Fyles";
        exec = "fyles";
        icon = "fyles";
        comment = "Share files with your contacts directly";
        categories = ["Network" "FileTransfer"];
      })
    ];

    installPhase = ''
      runHook preInstall

      mkdir -p $out/lib/fyles $out/lib/systemd/user $out/libexec/fyles
      cp -r app/. $out/lib/fyles/
      install -Dm755 bin/fyles-daemon $out/bin/fyles-daemon
      install -Dm644 share/fyles.png $out/share/icons/hicolor/512x512/apps/fyles.png

      substitute ${./fyles/daemon-start.sh} $out/bin/fyles-daemon-start \
        --subst-var-by shell ${runtimeShell} \
        --subst-var-by coreutils ${coreutils} \
        --subst-var-by xdgUserDirs ${xdg-user-dirs} \
        --subst-var out
      substitute ${./fyles/launcher.sh} $out/bin/fyles \
        --subst-var-by shell ${runtimeShell} \
        --subst-var-by coreutils ${coreutils} \
        --subst-var-by utilLinux ${util-linux} \
        --subst-var out
      substitute ${./fyles/wait-for-dns.sh} $out/libexec/fyles/wait-for-dns \
        --subst-var-by shell ${runtimeShell} \
        --subst-var-by coreutils ${coreutils} \
        --subst-var-by gnugrep ${gnugrep}
      chmod +x $out/bin/fyles-daemon-start $out/bin/fyles $out/libexec/fyles/wait-for-dns
      substitute ${./fyles/fyles-daemon.service} $out/lib/systemd/user/fyles-daemon.service \
        --subst-var-by exitAlreadyRunning ${toString exitAlreadyRunning} \
        --subst-var out

      runHook postInstall
    '';

    # Flutter finds its data/ beside the real executable, so the wrapper lives in bin/ instead.
    postFixup = ''
      makeWrapper $out/lib/fyles/Fyles $out/bin/fyles-app "''${gappsWrapperArgs[@]}"
    '';

    passthru = {inherit exitAlreadyRunning;};

    meta = {
      description = "Share files with your contacts directly, peer to peer";
      homepage = "https://fyles.app";
      license = lib.licenses.unfree;
      sourceProvenance = [lib.sourceTypes.binaryNativeCode];
      platforms = ["x86_64-linux"];
      mainProgram = "fyles";
    };
  }
