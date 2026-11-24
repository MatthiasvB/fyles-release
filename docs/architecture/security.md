# Security & Cryptography

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Threat Model

Fyles operates in a **zero-trust peer-to-peer** environment. There is no central server to mediate trust. The security model must handle:

- Untrusted network links (LAN or internet)
- Peers that may be impersonated
- Post-quantum adversaries (forward-looking)
- Session replay attacks
- Backup files that leave the device (cloud folders, USB sticks, mail)

---

## 2. Cryptographic Primitives

| Primitive | Algorithm | Purpose |
|-----------|-----------|---------|
| Symmetric encryption | ChaCha20-Poly1305 | Per-session message encryption (AEAD) |
| Key exchange | X25519 (Diffie-Hellman) + Kyber (KEM) | Deriving shared session keys via hybrid PQ scheme |
| Classical signing | Ed25519 | Contact identity verification |
| Post-quantum signing | Dilithium5 (CRYSTALS) | Quantum-resistant identity verification |
| Key derivation | HKDF (SHA-256) | Deriving symmetric keys from DH shared secret |
| Nonce management | Rolling nonce window | Replay protection without synchronized state |
| File stream keys | HKDF (SHA-256) over a per-attempt grant | One ChaCha20-Poly1305 key per direction of a file data stream |
| Backup encryption | Argon2id + XChaCha20-Poly1305 STREAM | Backup files sealed under a key derived from the user's backup password (see [ADR-0012](adr/0012-password-encrypted-backups.md)) |

This follows the approach described in the [IETF hybrid TLS draft](https://datatracker.ietf.org/doc/html/draft-ietf-tls-hybrid-design).

---

## 3. Contact Identity

Each contact (user) possesses a `ContactKeys` struct:

```rust
pub struct ContactKeys {
    pub private: ContactPrivateKeys,  // Dilithium5 secret + Ed25519 signing key
    pub public: ContactPublicKeys,    // Dilithium5 public + Ed25519 verifying key
}
```

- Keys are generated at first launch and stored in the SQLite database
- Public keys are shared via QR code or clipboard during contact exchange
- Both signature schemes must validate for a message to be accepted (defense in depth)

---

## 4. Session Lifecycle

### 4.1 Establishment

When two peers need to communicate, they negotiate a cryptographic session:

```
Peer A (initiator)                  Peer B (responder)
  │                                   │
  │──── AuthWireRequest ─────────────→│
  │     payload: sender               │
  │            + A's ephemeral X25519 │
  │            + ML-KEM public key    │
  │            + will_authenticate    │
  │     + ML-DSA & Ed25519 signatures │
  │                                   │  B verifies both signatures
  │                                   │  against A's contact keys
  │                                   │
  │←─── AuthWireResponse ─────────────│
  │     payload: sender               │
  │            + is_authenticated     │
  │            + B's ephemeral X25519 │
  │            + ML-KEM ciphertext    │
  │     + ML-DSA & Ed25519 signatures │
  │                                   │
  │  Both derive shared secret via X25519 + ML-KEM
  │  Both derive the ChaCha20 key via HKDF, salted
  │  with the concatenated handshake transcript
  │                                   │
  │←────── Encrypted traffic ────────→│
```

Signature verification is skipped where the peer's contact keys are unknown; the session is then
established at a lower `EstablishedSessionAuthentication` level rather than refused.

### 4.2 Signed Envelope

Every handshake message is a `Signed<P>`: a payload plus the two signatures over it.

```rust
pub struct Signed<P> {
    pub payload: P,
    pub pq_signature: PqSignatureWrapper,
    pub ed25519_signature: ed25519_dalek::Signature,
}

pub type AuthWireRequest<T>  = Signed<RequestPayload<T>>;   // sender + message
pub type AuthWireResponse<T> = Signed<ResponsePayload<T>>;  // sender + is_authenticated + message
```

**The signed bytes are derived, not hand-written.** They are
`bincode::serde::encode_to_vec(payload, BINCODE_CONFIG)` — the same `Serialize` impl that puts the
struct on the wire. A field added to a payload is therefore authenticated automatically.

`Signed::verify` is driven by the same encoding as `Signed::sign`, so verification cannot fall behind a
new field either.

This picks the better failure mode. With a hand-written encoder, forgetting to extend it breaks nothing:
both peers agree, everything keeps working, and the new field is simply never authenticated — a silent
security hole. With a derived encoding, forgetting to bump `SESSION_ESTABLISHMENT_PROTOCOL` breaks
loudly: peers on either side of the change still negotiate the same protocol string, then fail signature
verification because their derived bytes differ, and no session is established. Noisy and immediately
visible, with nothing left unsigned.

### 4.3 Key Derivation

```rust
let hk = Hkdf::<Sha256>::new(Some(transcript), &ikm);
hk.expand(SESSION_KDF_INFO, &mut okm).expect("works");
hk.expand(SESSION_ID_INFO, &mut id).expect("works");
```

where `ikm` is the ML-KEM-1024 shared secret followed by the X25519 shared secret, and `transcript` is
`request.transcript_bytes() ‖ response.transcript_bytes()` — byte-for-byte the same encodings the
signatures cover.

**Why `info` matters.** It is what domain-separates the two derivations: sharing one would make the id
*be* the key.

**The session id.** Both peers derive the same 32 bytes, and it is one-way from the PRK, so it names a
session without exposing anything about its key — safe to retain for the session's lifetime and safe to
log. It backs `Session`'s `PartialEq`, which the resend-after-session-error path uses to ask whether the
session it holds now is the one a failed request went out on.

**Raw secrets do not outlive the derivation.** Once the cipher exists, the shared secrets and the
intermediate key material are wiped: `SessionSecrets` clears its ML-KEM half on drop (the X25519 half
and the ML-KEM decapsulation key do it themselves, given their crates' `zeroize` features), and the
`ikm`/`okm` locals are `Zeroizing`. Those upstream features are load-bearing, so `crypto/src/session.rs`
carries compile-time assertions that fail the build if a dependency bump drops one.

**Why the transcript is the salt.** It is the per-session material the handshake already carries, so it
costs no extra wire bytes. A compiled-in constant was rejected: being invariant, it would separate
nothing that `SESSION_KDF_INFO` does not already separate.

**What this buys: no attack it stops today.** The key material is itself the IKM, so tampering with it
already makes the two sides derive different keys. What the transcript adds is coverage of the
handshake's **non-key bytes** — `will_authenticate_responder`, `sender`, `is_authenticated` — which are
not KDF inputs. That is future-proofing for that class of field. It does **not** stop a full MITM relay
running two complete handshakes, one with each side; only authentication does that.

### 4.4 Validity

| Parameter | Value |
|-----------|-------|
| Session validity | 30 minutes (`SESSION_VALIDITY_DURATION`) |
| Grace period for expired sessions | 60 seconds (`ACCEPT_SESSION_PAST_VALID_DURATION`) |
| Session construction timeout | 60 seconds (`SESSION_CONSTRUCTION_TIMEOUT`) |

### 4.5 Expired Session Fallback

When a message arrives, the `SessionResponder` tries decryption with multiple sessions (Chain of Responsibility pattern):

1. Current active session
2. Recently expired sessions (maintained in `ScopedExpiryMap`)

This handles the race condition where a message is sent just before a session expires.

---

## 5. Nonce Management

The `Dencryptor` uses a `NonceGeneratorValidator` with a **rolling nonce window**:

- Sender increments a counter for each message
- Receiver maintains a window of recently seen nonces
- Out-of-window or replayed nonces are rejected

Nonces are 8 bytes (u64), zero-padded to 12 bytes for ChaCha20's nonce requirement.

---

## 6. The `Encrypted<T>` Newtype

```rust
pub struct Encrypted<T: ?Sized> {
    nonce: u64,
    message: Vec<u8>,
    _type: PhantomData<T>,
}
```

Uses `PhantomData<T>` to carry the original type at compile time. `Encrypted<FileRequest>` and `Encrypted<Contact>` are distinct types — you cannot accidentally decrypt one as the other.

### Encrypt/Decrypt Traits

```rust
pub trait Encrypt: Serialize {
    fn encrypt(&self, dencryptor: &mut Dencryptor) -> Result<Encrypted<Self>, SerCryptError>;
}

pub trait Decrypt: DeserializeOwned {
    fn decrypt(&self, dencryptor: &mut Dencryptor) -> Result<Self::Payload, DeSerCryptError>;
}
```

Blanket implementations ensure any `Serialize` type can be encrypted, and any `Encrypted<T>` where `T: DeserializeOwned` can be decrypted.

### 6.1 File Data Streams

File data does not travel as `Encrypted<T>` messages. Each transfer attempt has a stream of its own
(see [Networking](networking.md#stream-transfer-flow)), keyed by the attempt's grant:

- **Grant.** The receiver draws a UUIDv4 attempt id and a 32-byte key from the OS RNG for every
  accept. The grant reaches the sender only inside the session-encrypted accept, so only the holder
  of that contact's PQ-hybrid session learns the key.
- **Keys.** HKDF-SHA256 over the grant key, salted with the attempt id, yields one ChaCha20-Poly1305
  key per direction (`SENDER_TO_RECEIVER_INFO`, `RECEIVER_TO_SENDER_INFO` in `crypto::stream`).
  Separate keys let both frame counters start at zero without ever reusing a nonce.
- **Frames.** Each frame is a 4-byte length, the ciphertext and a 16-byte tag. The nonce is a
  per-direction frame counter that is never sent. A dropped, replayed, reordered or altered frame
  fails authentication, so no replay window is needed. A length outside the frame bounds is refused
  before anything is buffered. The receiver accepts frames up to `MAX_INCOMING_FRAME_PAYLOAD`, well
  above the `STREAM_FRAME_PAYLOAD` it sends, so a later release can send larger frames without a
  protocol bump.
- **Admission.** The attempt id is sent in clear, so anyone able to read the transport could repeat
  it. A stream therefore takes over its reservation only once its first frame, `Begin`, authenticates.
  A stream that fails leaves the reservation in place for the genuine one.
- **Completion.** Only `End`, sent after the sender's last check on the source, finalizes a file, and
  only once exactly the declared size has been written. Only an authenticated `Committed` marks the
  file sent. Cutting a stream short is an interruption, never a completion.
- **Lifetime.** A grant lives as long as its attempt, not its session, so a long transfer outlives a
  session rotation without rekeying. A grant that no stream takes up within ten seconds is dropped.
  [ADR 0011](adr/0011-file-data-on-streams.md) records why.

---

## 7. Authorization Model

### 7.1 File Transfer Authorization

Authorization keys on the **`ContactId` carried by the session**, never on the `PeerId`. A `PeerId` is a
transport identity: self-chosen, unrelated to contact identity, and one contact legitimately has several
of them across devices. It proves nothing about who is on the other end.

A `ContactId` is `SHA-256(pq_verifying_key ‖ ed25519_key)` over the identity's public keys, rendered
as `fc1-` + unpadded base32 (`crypto::contact_id`, [ADR 0009](adr/0009-contact-id-derived-from-identity-keys.md)).
Everyone holding the public keys computes the same value, so no other key set can claim an id.
A contact share is signed by the keys it carries, so nobody can attach a name to keys they do not
hold. Wherever the keys travel — contact share, QR registration, out-of-band key fetch — the id is
derived on receipt rather than transmitted.

The `ContactId` comes from `Session::get_contact_id()`, which reads the session's
`EstablishedSessionAuthentication`. That variant is only set to `IKnowOther`/`WeKnowEachOther` when the
handshake signatures verified against that contact's stored public keys, so the identity is
cryptographically established rather than claimed. An unauthenticated session yields `None`.

When a file transfer request arrives:

1. The session supplies `Option<ContactId>` — `None` if the peer was never authenticated
2. `Filerequest::admits_sender` decides, for a new transfer and a continued one alike:
   - **Inactive**: nobody may send
   - **Public**: anybody may send, including `None`
   - **Audience**: the id must appear in the audience list, and `None` always fails
   - **Our own identity** may send into any active filerequest, whatever its audience
3. Continuation of an existing transfer additionally requires the `peer_id` *and* `contact_id` to match
   the ones recorded when the transfer started (`ReceivedFile::continuable_by`). The same sender may
   withdraw an unsettled transfer, and nobody else may (`ReceivedFile::withdrawable_by`).
4. Unauthorized requests get no response payload

When sending, the tracker binds each attempt to the contact behind the target filerequest. Before it
offers the file, it asks once whether we hold that contact's keys, counting our own identity. If we do,
the slot is pinned. Every response of any kind must then come from a session that proves the target
contact, or it resets the attempt instead of advancing it. The proof rests on the session handshake's
signatures, so it holds even if the transport's own authentication is broken. If we hold no keys, the
slot admits any session, and it pins as soon as one proves the target.

A file fails after repeated protocol violations, so only a violation the authenticated peer could have
caused counts toward that limit. That covers its `ProtocolViolation` answers, its authenticated
`Failed(Violation)` reports and our own findings on responses that passed the pin. Timeouts, transport
errors, errors that arrive unencrypted, responses that fail to decrypt and stream frames that fail
their tag never count. Anyone on the path can produce those, and counting them would let that person
fail the transfer. Internal errors on either side don't count either. A retry after the next lookup is
cheap.

### 7.2 Device Joining (Identity Cloning)

New devices join by securely cloning the `ContactKeys` (the private and public keypairs) from an existing device. Because both devices hold the same cryptographic material, they are mathematically recognized as the same "contact" by the rest of the network — the `ContactId` follows from the keys rather than being cloned alongside them. There is no central synchronization of a "device list."

**Primary Method (Full QR Transfer):**
The existing device displays a series of chunked QR codes containing the full `ContactKeys` payload. The new device scans these to immediately clone the identity.

**Convenience Extension (Network Transfer):**
If scanning large amounts of data via QR is inconvenient, the keys can be shared securely over the local network:
1. The existing device generates a random byte challenge, valid for `SELF_CONTACT_INVITE_TTL`
   (60 seconds — deliberately short, since a stale code that still works hands over the identity).
2. The challenge and connection details are shared via as part of the first QR code.
3. The new device connects and proves knowledge of the challenge.
4. Upon verification, the existing device securely transmits the full `ContactKeys` over the encrypted session and invalidates the challenge.

## 8. Pro Licensing

The pro-licensing subsystem reuses the contact dual-signature scheme through the
generic envelope `crypto::signed::Signed<P>`: license certificates are dual-signed by
the license server's own `ContactKeys`, and license-server requests are dual-signed by
the client's identity keys, verified against the server's stored keys-of-record.
Certificates bind to the identity's `ContactId`, which is derived from the identity's public
keys, so the server can check that a presented key set actually owns the id it claims. They
are verified offline. Evaluation fails open by design, and no PII is ever stored. Key rotation
is unimplemented and will change the identity when it arrives
([ADR 0009](adr/0009-contact-id-derived-from-identity-keys.md)). There is no replay layer: the
authenticated endpoints are idempotent, so a timestamp or nonce would be ceremony. Transport is
relayed through the frontend, where the core signs and verifies but links no TLS stack, so
authenticity rests entirely on the dual-signed certificate rather than on the channel. See
[Pro Licensing](pro-licensing.md) and ADRs
[0003](adr/0003-hybrid-signatures-for-license-certificates.md)–[0006](adr/0006-license-server-placement-transport-replay.md)
and [0009](adr/0009-contact-id-derived-from-identity-keys.md).
