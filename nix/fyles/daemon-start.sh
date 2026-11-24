#!@shell@
# The one place that decides where the daemon keeps its data, listens and puts received files.
# FYLES_OUTPUT_DIR and FYLES_LISTEN_PORT override, and the daemon reads the latter itself.
set -eu

data="${XDG_DATA_HOME:-$HOME/.local/share}/fyles"
# The app looks for the socket here, and falls back to its own data folder without a runtime dir.
if [ -n "${XDG_RUNTIME_DIR:-}" ]; then
  socket="$XDG_RUNTIME_DIR/fyles.sock"
else
  socket="${XDG_DATA_HOME:-$HOME/.local/share}/app.fyles.fyles/fyles.sock"
fi
output="${FYLES_OUTPUT_DIR:-$(@xdgUserDirs@/bin/xdg-user-dir DOWNLOAD)/Fyles}"

@coreutils@/bin/mkdir -p "$data" "$(@coreutils@/bin/dirname "$socket")" "$output"
exec @out@/bin/fyles-daemon \
  --endpoint "uds:$socket" \
  --db-path "$data/fyles.db" \
  --output-dir "$output"
