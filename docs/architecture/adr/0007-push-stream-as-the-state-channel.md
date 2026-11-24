# 0007 — The push stream is the state channel, and the wire keeps its replies

## Status

Accepted (2026-08)

## Context

Desktop Fyles runs one daemon with any number of client windows attached. Request/response
mutations only inform the window that issued them, so a change made in one window was invisible
in another. A 10s page-scoped poll and manual refresh buttons masked the divergence.

The masking was partial. Received-file arrivals were already handled properly — a status push
followed by a scoped refetch — while a filerequest created in one window never appeared in
another at all, because the filerequest list was read once at startup and never again. What the
poll actually bought was narrow: self-contact display, received-file deletions, and
remote-filerequest/pending-file mutations.

Two forces pull against simply making every endpoint fire-and-forget:

- Some calls genuinely need their result, most sharply the license check, whose response *is* the
  body of the next request.
- Automation and testing depend on request/response. `e2e_test/e2e_file_loop.sh` drives the daemon
  with `grpcurl`. A partial client that simulates a scenario should not have to implement stream
  handling to be useful.

## Decision

Make the `ServerMessage` stream the authoritative channel for client state, and **keep every RPC's
full response on the wire**. The discard happens one layer up, in the Dart `ProtobufClient`, where
mutations collapse to a `Future<bool>` success flag. Reads keep returning data.

`AppBloc` holds no optimistic state. The service pushes on every change to observable state,
including changes the receiving window made itself, and the push is the only thing that moves
state.

A push is **scoped to the entity that changed**, matching what the RPC's own response would carry.
Whole-concern state goes out only where the changed set is genuinely unknown, which is the
sync-convergence path: a merge with another device reports that the document moved, not which
entries moved with it.

## Consequences

- A missing push is visible immediately, on the window that made the change, instead of being
  silently correct there and wrong everywhere else.
- Reconnect becomes load-bearing. Nothing replays pushes missed while the stream was down and no
  poll remains, so every re-attach must re-run `bootstrap()`.
- Whole classes of code disappear: optimistic patches, server-disagreement reconciliation, and
  refetches chained onto mutation replies.
- Automation is unaffected. The proto surface is unchanged, so existing scripts keep working
  verbatim, and read RPCs the Flutter app does not call must be kept rather than pruned.
- A malformed row from the service is a bug in a component that ships with the client, so the
  conversion throws rather than skipping the row. Silently shortening a list hides the defect.
- Every future addition to observable state owes a push. `client_push.rs` exists so that
  obligation has one place to live and can be audited against the `ServerMessage` oneof.

## Alternatives considered

**Drop response payloads from the proto itself.** The cleanest expression of the intent, and the
reason it lost: it would force every test, script, and partial client to become stream-aware, and
it discards a working request/response surface that costs nothing to keep.

**Keep the poll as a safety net.** Rejected as the permanent answer because a poll hides exactly
the bug being fixed — it makes a missing push look like a slow one. The manual refresh button is
kept for now instead, repointed at a full resync and, in debug builds, raising a snackbar whenever
that resync finds a difference. It comes out once that has stayed quiet in daily use.

**Optimistic updates with the push as confirmation.** Rejected because it keeps two code paths per
mutation and restores the invisibility: the originating window would look correct whether or not
the push exists. The cost of dropping it is one local round trip over a unix socket.
