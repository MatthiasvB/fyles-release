#!@shell@
# The daemon reads the DNS servers once, as it builds its network node, and fails without one. At
# login the network may not have set them yet. Gives up after 30 seconds, so an offline start is
# delayed rather than prevented.

for _ in $(@coreutils@/bin/seq 30); do
  @gnugrep@/bin/grep -qs '^nameserver' /etc/resolv.conf && exit 0
  @coreutils@/bin/sleep 1
done
