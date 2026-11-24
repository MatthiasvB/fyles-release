# Proofs

Tests that established how something works, kept as documentation. Nothing references this
directory, so they neither compile nor run with the rest of the crate.

| Proof | What it shows |
|---|---|
| `schema_evolution_spike.rs` | How an `autosurgeon` struct hydrates from a document written under a different schema: added fields, removed fields and enum variants. |
| `derived_id_convergence.rs` | Two devices paired and converged at schema 1.23.0, migrated to 1.24.0 while disconnected, then merged. Both land on the same key-derived contact ids, keep each other's unsynced entries, and drop remote filerequests whose contact is unknown. |

## Running one

Declare it as a test module next to the code it was written against, then run `cargo test`.

`schema_evolution_spike.rs`, at the end of `src/core/sync.rs`:

```rust
#[cfg(test)]
#[path = "../../proofs/schema_evolution_spike.rs"]
mod schema_evolution_spike;
```

`derived_id_convergence.rs`, at the end of `src/library/sqlite/migrations.rs`:

```rust
#[cfg(test)]
#[path = "../../../proofs/derived_id_convergence.rs"]
mod derived_id_convergence;
```

A proof is not maintained alongside the code, so it may need adjusting to compile. It last passed
against schema 1.24.0.

## `testdata/`

Used by `derived_id_convergence.rs`. Four contacts appear under a slug:

| Slug | UUID id before 1.24.0 | Where it is |
|---|---|---|
| `shared` | `2f8a1c00-0000-4000-8000-000000000001` | Both devices, synced |
| `keyless` | `2f8a1c00-0000-4000-8000-000000000002` | Both devices, synced |
| `a_only` | `2f8a1c00-0000-4000-8000-000000000003` | Device A only, written after the sync |
| `b_only` | `2f8a1c00-0000-4000-8000-000000000004` | Device B only, written after the sync |

`keyless` differs from `shared` only in intent. The proof gives neither device any key bytes in
its `contacts` table, so every contact migrates from the fingerprint in the synced document. The
slug marks the case that matters in practice, a contact synced from another device whose keys never
arrived.

| File | Contents |
|---|---|
| `<slug>.pq.bin` | The contact's ML-DSA-87 verifying key, 2592 raw bytes. Only the proof reads it, to compute the id the migration should produce. |
| `<slug>.ed.bin` | The contact's Ed25519 public key, 32 raw bytes. |
| `contacts_a.v1.automerge` | Device A's contacts document in the pre-1.24.0 shape, keyed by UUID, each entry holding a name and the SHA-256 key fingerprint. Holds `shared`, `keyless` and `a_only`. |
| `contacts_b.v1.automerge` | Device B's contacts document. Holds `shared`, `keyless` and `b_only`. |
| `remote_fr_a.v1.automerge` | Device A's remote filerequests: `rfr-shared` (contact `shared`), `rfr-a` (contact `a_only`) and `rfr-orphan`. |
| `remote_fr_b.v1.automerge` | Device B's remote filerequests: `rfr-shared`, `rfr-b` (contact `b_only`) and `rfr-orphan`. |
| `ids.v1.txt` | The slug-to-UUID table above, plus `orphan`, the UUID `rfr-orphan` names. No contact has that id. |

### How they were made

A one-off test ran against the 1.23.0 code. It generated four key sets, and it wrote `shared` and
`keyless` into device A's contacts document and `rfr-shared` and `rfr-orphan` into its remote
filerequests document. It then drove the real sync state machine between A's and B's documents
until they converged. Afterwards each device made one local write the other never saw: a contact
(`a_only`, `b_only`) and a remote filerequest naming it (`rfr-a`, `rfr-b`). The saved documents
therefore carry genuine shared history followed by divergence.
