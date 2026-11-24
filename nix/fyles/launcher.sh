#!@shell@
# Starts the daemon in the background and opens the app. A daemon that is already running, from
# here or from the systemd unit, holds the database's lock, so the new one exits at once.
set -eu

state="${XDG_STATE_HOME:-$HOME/.local/state}/fyles"
@coreutils@/bin/mkdir -p "$state"
@utilLinux@/bin/setsid -f @out@/bin/fyles-daemon-start >>"$state/daemon.log" 2>&1 </dev/null
exec @out@/bin/fyles-app "$@"
