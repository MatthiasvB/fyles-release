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
Peer A                              Peer B
  │                                   │
  │──── SessionEstablishment ────────→│
  │     (A's ephemeral X25519 pub     │
  │      + Kyber public key)          │
  │                                   │
  │←─── SessionEstablishment ─────────│
  │     (B's ephemeral X25519 pub     │
  │      + Kyber ciphertext)          │
  │                                   │
  │  Both derive shared secret via X25519 + Kyber
  │  Both derive ChaCha20 key via HKDF
  │                                   │
  │←────── Encrypted traffic ────────→│
```

### 4.2 Validity

| Parameter | Value |
|-----------|-------|
| Session validity | 30 minutes (`SESSION_VALIDITY_DURATION`) |
| Grace period for expired sessions | 60 seconds (`ACCEPT_SESSION_PAST_VALID_DURATION`) |
| Session construction timeout | 60 seconds (`SESSION_CONSTRUCTION_TIMEOUT`) |

### 4.3 Expired Session Fallback

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

When a file transfer request arrives:

1. Sender's `PeerId` is looked up against known contacts
2. The target `filerequest_id` is checked:
   - **Public filerequest**: anybody may send
   - **Audience filerequest**: sender must be in the audience list
3. If unauthorized, the transfer is rejected with `FileRespons::Rejected`

### 7.2 Device Joining (Identity Cloning)

New devices join by securely cloning the `ContactKeys` (the private and public keypairs) from an existing device. Because both devices hold the same cryptographic material, they are mathematically recognized as the same "contact" by the rest of the network. There is no central synchronization of a "device list."

**Primary Method (Full QR Transfer):**
The existing device displays a series of chunked QR codes containing the full `ContactKeys` payload. The new device scans these to immediately clone the identity.

**Convenience Extension (Network Transfer):**
If scanning large amounts of data via QR is inconvenient, the keys can be shared securely over the local network:
1. The existing device generates a random byte challenge (valid for 30 minutes).
2. The challenge and connection details are shared via as part of the first QR code.
3. The new device connects and proves knowledge of the challenge.
4. Upon verification, the existing device securely transmits the full `ContactKeys` over the encrypted session and invalidates the challenge.
