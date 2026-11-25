# 0014 — One GitHub release carries every platform's build

## Status

Accepted (2026-11).

## Context

Linux ships as a prebuilt tarball that fyles-release's flake packages ([0013](0013-linux-through-nix.md)).
Android should also be installable without Google Play, and Windows and macOS will follow. Flutter
does not cross-compile desktop apps, so each of those builds on a machine of its own.

## Decision

**One release per version.** The tag is `v<version>` from `fyles/pubspec.yaml`, in fyles-release.
Each platform has a Python script, `release/<platform>.py`, that builds into
`fyles/build/release/v<version>/` and takes from `release/common.py` the version, the release URL,
the Rust path remaps, the Flutter copy under `/tmp` and the check for traces of the build machine.
Python stops at the first failing command (`check=True`), inspects APKs and writes the tarball with
its standard library, and runs on every machine a release is built on. Linux creates the release,
because its tag must point at the commit whose flake names the tarball. `release/linux.py` refuses
to build a version whose release already carries the tarball, since the flake pins that file by
hash. The other platforms add their assets with `gh release upload`.

GitHub shows a SHA-256 digest beside every asset, so the release carries no checksum files of its
own. Those would sit on the same page and prove no more. Authenticity comes from the APKs' signing
certificate, whose fingerprint the README states, and from the tarball's hash in the flake.

**Android: an APK for each ABI, plus a universal one.**
- `arm64-v8a`, `armeabi-v7a` and `x86_64` each get an APK of their own, and the universal APK
  carries all three.
- Each APK is built with `--target-platform`, from which `fyles/android/app/build.gradle` derives
  Gradle's `abiFilters`. Those also keep out the native code that plugins ship for every ABI.
  Flutter's own filtering would put every ABI back after `build.gradle`, so `gradle.properties`
  turns it off with `disable-abi-filtering`. All the APKs keep the versionCode from
  `pubspec.yaml`, so any variant updates any other.
- They are signed with the release key from `fyles/android/key.properties`.
- The script refuses to finish on a missing `key.properties`, a debug certificate, APKs with
  different certificates, an APK with native code for other ABIs than its own or without Flutter
  or the backend for one of them, and any trace of the build machine.

## Consequences

- Users find every platform's build in one place.
- Every release needs the Linux build first, since its steps create the tag.
- Whether a GitHub APK and a Play install update each other depends on Play App Signing. If Play
  signs with a key of its own, the two are separate channels, and switching means uninstalling. The
  README says so and points to the in-app backup.
- Building in the `/tmp` copy also copies the keystore there for the length of the build.
- Every Android build, not only a release, gets its `abiFilters` from `build.gradle`. A build for
  all platforms, such as the Play bundle, keeps the same three ABIs that Flutter's filtering
  allowed.
- The release folder moved from `fyles/build/release-linux/` to `fyles/build/release/v<version>/`.

## Alternatives considered

- **`flutter build apk --split-per-abi`.** It gives every split a versionCode of its own (1000 times
  the ABI's code, plus the app's). A user on a split could then never move to the universal APK of a
  later version, or back.
- **Checksum files beside the assets.** GitHub already shows each asset's digest, and a file on the
  same page adds nothing an attacker could not replace with it.
- **Bash.** It lacks real lists and maps, so quoting and helper tricks crowd out the steps. Fish
  reads better but has no `set -e`, so every command would need its own error check.
- **A release per platform.** Users would search several pages, and only Linux needs its tag on a
  particular commit.
