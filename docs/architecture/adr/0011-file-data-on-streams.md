# 0011 — File data travels on one authenticated stream per attempt

## Status

Accepted (2026-10)

## Context

Every 512 KiB chunk of a file was its own request-response exchange on `/file-push/0.2.0`. Each one
cost a fresh substream with protocol negotiation, a registry entry with a timeout, a nonce from the
session's replay window, a response that had to be claimed and checked, and a round trip through the
event loop. Eight ran at once, so the sender kept a ring of slots and the receiver a reorder buffer.
Much of the transfer code existed to tolerate chunks and answers that arrived late, out of order, or
from an earlier attempt.

## Decision

**Control stays on request-response, the bytes move to a stream.** Offer, continue, abort and
protocol violation stay request-response on `/file-push/0.3.0`. They are rare, need correlation and
timeouts, and the checks against an impostor answering for the peer apply to them unchanged. An abort
must also reach the receiver when no stream exists. The data and the completion travel on one
stream per attempt, `/fyles/file-data/0.1.0`, so the stream's lifetime is the attempt's and no late
data from an earlier attempt can exist.

**Completion travels with the data.** `Done` on its own channel could overtake the last frames. The
sender ends the stream with `End` after its final check on the source, and the receiver finalizes on
`End`, not on reaching the declared size. A first version without `End` committed a file whose source
changed after the sender's last read, because the receiver finalized before that check ran.

**The receiver mints a key per attempt.** It sends the key inside the session-encrypted accept,
together with an attempt id. No session secret is reused, and only the holder of the contact's
PQ-hybrid session can produce frames. The key lives as long as its attempt, not its session. It
protects nothing but that attempt's bytes, it arrived over the PQ session, and binding it to the
session's 30 minutes would interrupt every long transfer at each rotation for no gain.

**An unused grant expires.** The receiver holds a reservation, with its part file open, for ten
seconds. A stream that names it must authenticate `Begin` within that time. When none does, the
reservation and its key are dropped. The part file and the record stay, as after any interrupted
attempt, and the sender resumes later with a fresh grant. A newer offer from the same peer replaces
the reservation at once.

**Version negotiation is libp2p's.** The payload inside `Encrypted` is bincode, which is positional.
A new field in `NewTransfer` would break old decoders, and a new variant would fail to decode there
and come back as a decryption error, which counts as a retry. So v2 is a separate protocol with its
own types. A sender that meets `UnsupportedProtocols` offers the same file over v1, without a retry
or a reset, and keeps doing so for that peer for ten minutes.

**No file data over relays, and no dial from the stream layer.** The fork's libp2p-stream is
configured to use neither relayed connections nor dialing, as request-response already was. The file
tracker's plugin owns dialing.

### Recorded departures from "types express only valid states"

- The receiver keeps a chunked map and a stream map side by side, and a peer may appear in at most
  one of them. Every offer goes through `clear_stream` to keep it so. A single map would idle-expire
  a healthy stream whose task never touches it. The two maps become one when v1 is retired.
- The sender's slot holds either path. A response of the other protocol finding it is treated as a
  protocol violation, like the other mismatches the tracker already guards against. This ends with
  v1's retirement.

## Consequences

- The reorder buffer, the slot ring, the per-chunk claims, timeouts and responses, and the per-chunk
  lock and event-loop round trip are gone from the stream path.
- A transfer attempt now runs as a task outside the event loop. Clearing the sender's slot cancels it,
  and the task reports a single outcome that the tracker ignores unless the slot still holds that
  attempt.
- The receiver may hold its lock only to read or change its per-peer state. A part file is never
  reopened while a task that might still write to it is alive.
- Until v1 is retired, both protocols and the fallback stay in the code. The chunked path receives
  fixes only.

## Alternatives considered

- **Everything on the stream.** One channel per transfer would remove the remaining join between the
  accept and the stream. It would rebuild request-response inside the stream, and an abort with no
  stream open would still need a channel. Worth revisiting once v1 is gone.
- **A digest of the whole file.** It catches a source that changed only after one full read, which on
  a resumed transfer is the case that matters. The size and modification-time stamp, checked before
  resuming, during sending and before `End`, catches it as it happens.
