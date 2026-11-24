# 0009 — The ContactId is derived from the identity's public keys

## Status

Accepted (2026-09). Supersedes [0002](0002-license-bound-to-contact-id.md).

## Context

A `ContactId` names an identity, and it is a primary key, an automerge map key and a `.part`
filename segment. A self-assigned identifier with no relationship to the identity it names can be
squatted: an adversary publishes a contact carrying somebody else's id, and anyone holding that
entry can no longer add the real peer.

[0002](0002-license-bound-to-contact-id.md) chose a UUID deliberately, and explicitly rejected
binding to a key fingerprint, on the grounds that key material is not a stable anchor. The ml-dsa
crates may change encodings, and a full rotation is conceivable, so a fingerprint binding would
orphan every license. That argument is about the *encoding* drifting under a fixed identity, and
it is answered by versioning the derivation rather than by refusing to derive at all.

Deriving the id does not by itself stop somebody offering a third party's public keys as their
own. Those keys derive the third party's id, so the entry lands on it with a name the adversary
chose. Contact material reaches a device two ways, and both must therefore prove that the sender
holds the private keys for the public keys it offers:

- **Offline**: QR frames or a base64 paste, imported through a confirm dialog.
- **Network**: a one-time challenge shown in the sharer's QR code, redeemed over a session.

## Decision

- **The id is computed from the public keys and nothing else.** It is
  `SHA-256(pq_verifying_key ‖ ed25519_key)`, the same fingerprint the contacts document already
  carries. The type has no other constructor, and parsing its text form validates.
- **The text form is `fc1-` + unpadded RFC 4648 base32** of the digest, uppercase, decoded
  strictly. The prefix makes a future derivation change detectable instead of silent. The
  strictness matters because an id is an automerge map key, and two spellings of one digest
  would be two entries that never converge.
- **Where the keys travel, the id does not.** Every structure that carries keys, including the
  protobuf messages and the license identity, exposes the id as derived from them. There is
  therefore no one-time verification to perform and no way to skip one.
- **The self contact's stored id is dropped from the schema.** It is a function of the signing
  keys in the same row. Absent keys mean absent identity.
- **Every contact share is signed by the keys it carries**, with both signatures of the dual
  scheme. Verification needs nothing but the share itself, and offering keys you do not hold
  cannot produce a share that verifies. The signature does not bind the share to an exchange. On
  the network path, libp2p request-response already ties the answer to the request that asked
  for it, and forging that answer means breaking the Noise session, which would also expose any
  challenge the share could have been bound to.
- **The sharer answers only a challenge it issued.** A network share still requires having seen
  the sharer's QR code.
- **The confirm dialog shows only what a signature covers.** Decoding a share for the dialog
  verifies it without storing anything, and registering it verifies it again. The two checks are
  independent, so neither can be removed on the assumption that the other still runs.
- **A local rename travels beside the share**, never inside it. The share keeps verifying, and the
  name its key holder attested to is stored unless the user replaced it.
- **Rotation is not implemented.** Nothing in the client rotates keys, so the license server has no
  key succession endpoint. Such a route would install keys disagreeing with the id they update.
  When rotation is needed it arrives as a succession statement, the new key set signed by the old
  keys, which moves contacts *and* the license to the new id. It gets its own ADR.
- **The session-establishment protocol goes to `/session-establishment/0.2.0`.** Old and new
  builds cannot pair. Contacts re-pair once both sides update.
- **Share and invite messages live in a separate compatibility proto.** Devices exchange them
  through QR codes and pastes, outside any versioned protocol, so a device can read one that a
  different version wrote. Their field numbers are never reused.

## Consequences

- Choosing an id independently of the keys is unrepresentable. The recovery gap recorded in
  `pro-licensing.md` §6 closes, since a stolen certificate presented with fresh keys names an id
  those keys do not derive.
- A contact's id is computable by anyone holding its public keys. A device that learned a contact
  through sync identifies it from the fingerprint alone, with no key bytes.
- An unverified contact id never enters the system. The id a peer claims in its session handshake
  is dropped as soon as the handshake has used it to look up keys, by design and permanently. Only
  a verified share or verified session authentication produces an id anything downstream can
  record or authenticate against.
- A shared contact carries both signatures, 4627 bytes of ML-DSA-87 and 64 bytes of Ed25519, on a
  blob of roughly 2.6 KB. That is about three QR frames, which the multi-frame flow handles.
- Two contact rows can no longer disagree about one identity. A share that verifies names the id
  its keys derive, so importing the same identity twice is an upsert, not a collision.
- The license identity codec becomes `FYLES-ID2:`, and exported `FYLES-ID1:` strings stop parsing.
  No license data migrates because the license server is not yet deployed.
- Rotating keys is a change of identity to *every* contact, not just to the license server. That
  is deliberate, and it stays unsolved until succession exists.
- Two devices re-keying offline both write the shared entry, so those writes are concurrent. A
  contact renamed between migrating and the first post-migration sync can lose to the other
  device's migration write. The documents still converge, and nothing resurrects. This is inherent
  to re-keying a CRDT map offline.

## Explicitly not solved

**Re-presenting somebody else's genuine share.** A share is public bytes, so anyone holding a copy
can hand it to a third party, offline or as the answer to a network exchange, and no signature
distinguishes that from the original. Copying a QR code off a screen is indistinguishable from
being shown it. The harm is bounded. The share names a real identity the copier cannot
authenticate as, so the result is an unrequested but genuine contact, with no authority over it.
This is the guarantee every contact app gives, and it is not worth further machinery.

## Migrating the contacts document

The contacts and remote-filerequest documents are rekeyed at schema 1.24.0, following the rules in
[backend.md §5.8](../backend.md#58-migrating-a-synced-document).

**Destructive limitation.** The migration drops every remote filerequest whose contact is not in
the contacts document. That is a normal state in the app, so live data is lost. It is accepted
because the app is in beta.

Convergence is verified against v1 document fixtures captured from a real pre-migration sync
exchange. The proof and its fixtures live in `fyles_service/core/proofs/`, outside the normal test
run.

## Alternatives considered

- **Keep a self-assigned id and verify the binding once at contact-add.** It leaves the id and the
  keys as two values that can disagree, so every later reader has to trust that the check
  happened. Deriving removes the pair.
- **Keep a separate stable anchor for the license.** This preserves 0002's property, at the cost
  of reintroducing exactly the unverifiable self-assigned identifier this change removes, on the
  license server.
- **A dual-lookup transition keyed on a recorded legacy id.** It would let old and new builds
  interoperate, but adds transitional state to every contact lookup and a later removal. The
  protocol is pre-1.0, and migration 1.19.1→1.20.0 already forced a full re-pair.
- **Domain-separating the hash inputs.** Both inputs are fixed-length, so the concatenation is
  already unambiguous. Changing the bytes would have cost the migration its offline path for
  contacts whose keys are not held locally.
- **Keep the session's claimed id and check the share's keys against the handshake signature.**
  This uses a signature that already exists, at no extra round trip. It was rejected because an
  unverified id must not exist anywhere past the handshake. Dropping it as early as possible
  guarantees that no later code, correct or not, can record it or authenticate against it. The
  security boundary is enforced by the absence of that information, not by every consumer
  remembering to check it.
- **Bind a network share to the challenge the scanner sent.** It suggests a guarantee it does not
  add, since request-response already correlates the answer with the request. It also needs state
  on the scanner and a binding variant that the offline path cannot check.
- **Leave the name unsigned** so it could be edited in place. Smaller, and the name is
  discardable anyway, but it gives up attestation for nothing once a rename rides alongside.
- **Verify only at registration**, letting the dialog show unverified bytes. One check instead of
  two, at the cost of showing the user a name no signature covers.
