# Security & Cryptography

> **Parent:** [Architecture Overview](../ARCHITECTURE.md)

---

## 1. Threat Model

Fyles operates in a **zero-trust peer-to-peer** environment. There is no central server to mediate trust. The security model must handle:

- Untrusted network links (LAN or internet)
- Peers that may be impersonated
- Post-quantum adversaries (forward-looking)
- Session replay attacks

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
`ikm`/`okm` locals are `Zeroizing`. Those upstream features are load-bearing, so `p2p/src/crypto.rs`
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

---

## 7. Authorization Model

### 7.1 File Transfer Authorization

Authorization keys on the **`ContactId` carried by the session**, never on the `PeerId`. A `PeerId` is a
transport identity: self-chosen, unrelated to contact identity, and one contact legitimately has several
of them across devices. It proves nothing about who is on the other end.

The `ContactId` comes from `Session::get_contact_id()`, which reads the session's
`EstablishedSessionAuthentication`. That variant is only set to `IKnowOther`/`WeKnowEachOther` when the
handshake signatures verified against that contact's stored public keys, so the identity is
cryptographically established rather than claimed. An unauthenticated session yields `None`.

When a file transfer request arrives:

1. The session supplies `Option<ContactId>` — `None` if the peer was never authenticated
2. `FilerequestAccess::is_accessible_by(contact_id)` decides:
   - **Public**: anybody may send, including `None`
   - **Audience**: the id must appear in the audience list; `None` always fails
3. Continuation of an existing transfer additionally requires the `peer_id` *and* `contact_id` to match
   the ones recorded when the transfer started
4. Unauthorized requests get no response payload

### 7.2 Device Joining (Identity Cloning)

New devices join by securely cloning the `ContactKeys` (the private and public keypairs) from an existing device. Because both devices hold the same cryptographic material, they are mathematically recognized as the same "contact" by the rest of the network. There is no central synchronization of a "device list."

**Primary Method (Full QR Transfer):**
The existing device displays a series of chunked QR codes containing the full `ContactKeys` payload. The new device scans these to immediately clone the identity.

**Convenience Extension (Network Transfer):**
If scanning large amounts of data via QR is inconvenient, the keys can be shared securely over the local network:
1. The existing device generates a random byte challenge, valid for `SELF_CONTACT_INVITE_TTL`
   (60 seconds — deliberately short, since a stale code that still works hands over the identity).
2. The challenge and connection details are shared via as part of the first QR code.
3. The new device connects and proves knowledge of the challenge.
4. Upon verification, the existing device securely transmits the full `ContactKeys` over the encrypted session and invalidates the challenge.
