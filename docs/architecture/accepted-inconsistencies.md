# Accepted Inconsistencies

Known states the code can reach that are wrong, and that we leave in place on purpose because a fix
costs more than the case is worth. Each entry says how the state arises, what it breaks, what limits
it, and what a fix would take. Remove an entry once it is fixed.

## A session from before two peers knew each other can outlive the contact exchange

A session authenticates the peer only if its keys were known when the session was established. When
a contact is added through a share code redeemed over the network, the redeemer knows the peer and
discards the session it redeemed the code over. The sharer's next request on its own copy is
answered `MissingSession`, and it establishes a new session, which authenticates what each side
knows by then.

Every other way a contact arrives leaves the old session in place, and the share code leaves a short
gap of its own. Requests on such a session carry no authenticated contact, so a private file request
refuses them, and a sender that holds the owner's keys logs "Contact verification failed". Three
cases remain:

- **A QR code exchange.** Two devices meet on a network before either knows the other, for example
  through a public file request, and establish a session that authenticates neither. One or both
  leave, they exchange contacts by QR code, and they meet on the network again. A scanned contact
  comes with no peer, so there is no session to drop.
- **A contact synced from an owned device.** A contact added on one device reaches the user's other
  devices through sync. If one of those already holds an unauthenticated session with that contact's
  peer, it keeps using it. Sync brings the contact, not the peer it was learned from.
- **A request between storing the contact and discarding the session.** The node discards the
  session in a task of its own, a few milliseconds after the brain has stored the contact. A request
  from the sharer that arrives in between still decrypts with the old session and is refused as
  unauthenticated. A refusal is not a session error, so nothing sends it again, and the file ends as
  rejected. This is very unlikely to be hit: the sharer would have to send within milliseconds of
  answering the code, and nothing does that on its own, because it holds no file request of the
  redeemer yet.

**What limits it:** a session is usable for 30 minutes after it is established (`SESSION_LIFETIME`
in `p2p/src/constants.rs`). After that, the next request establishes a new, authenticated session.
Each case needs the stale session to be used inside that window.

**What a fix would take:** upgrading a session in place instead of dropping it. The handshake
already carries the peer's signature over its transcript, under the contact ID it claims. If the
session kept that signature, adding a contact by any path could verify it against the new keys and
mark the session as authenticating that contact. That would also make discarding the session after
a share code unnecessary, and with it the failed first request it costs the sharer.
