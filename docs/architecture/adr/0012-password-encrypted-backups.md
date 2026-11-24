# 0012 — Backups are encrypted with a password the device remembers as a key

## Status

Accepted (2026-11)

## Context

A backup is the whole local database: the identity's private keys, contacts, file requests and
the sync documents. It used to be zstd-compressed and nothing else, so anyone holding the file
could act as the user. Backups end up in cloud folders, on USB sticks and in mail, which makes
the file the easiest way to steal an identity.

Encrypting it needs a secret that survives the loss of the device, because restoring on a new
device is the point of a backup. That rules out a key the device keeps for itself. The secret has
to be in the user's head, so it is a password, and a password nobody types for months is a
password nobody remembers.

## Decision

**A per-device password, set on the first export.** Exporting with no password set opens the
dialog that sets one, and the export follows. The password belongs to the device and is never
synced. Each device exports its own database, and nothing secret travels between devices.

**The device keeps the derived key, never the password.** The single-row `backup_secret` table
holds an Argon2id salt, its parameters and the 32-byte key derived from them. Exports need no
prompt, and re-verification derives the key again and compares in constant time. Reading the
device database never reveals a password the user might reuse elsewhere. That database already
holds the plaintext identity, so storing the key there exposes nothing new. The table is part of
the backup, and that is deliberate: a restore needs the password anyway, and the restored row is
exactly the key for the password just typed.

**Argon2id with 64 MiB, 3 passes and 1 lane.** This is RFC 9106's memory-constrained profile and
above OWASP's minimum of 19 MiB and 2 passes. It costs about 0.3 to 1 s on a phone, which is paid
when setting, verifying or restoring, never per export. The parameters travel in every backup
header, so they can be raised later without stranding old files. A header asking for more than
256 MiB, 16 passes or 8 lanes is refused, so a crafted file cannot exhaust a phone's memory.

**Format.** `FYLESBK`, a version byte, the three Argon2 parameters, the 16-byte salt, a fresh
19-byte nonce prefix and a 16-byte key check. The zstd stream follows, sealed with
XChaCha20-Poly1305 in the STREAM construction (64 KiB chunks, the whole header as associated
data on each chunk). HKDF-SHA256 splits the stored key into the encryption key and the key check.
The key check tells a wrong password apart from a damaged file before any chunk is opened. The
final-chunk flag catches a file cut off at a chunk boundary. The code is `crypto/src/backup.rs`.

**The file is named `{date}_{time}_fyles.bak.crypt`, and the header decides.** Core owns the name
and hands it to the `HostController`. The local time down to the second keeps every export in its
own file, so a second export on the same day neither overwrites the first nor gets a numbered copy. The Android picker returns a content URI without a name, so
restore always reads the first bytes. A renamed file therefore still restores correctly.

**Re-verification on growing gaps.** After 1 day, then 7, 30 and 90, the app asks the user to
retype the password when it opens or resumes. A correct first answer moves to the next gap. A
correct answer after a wrong one moves back one gap. "Later" postpones by a day. An attempt only
counts while a check is due, so confirming the current password before changing it leaves the
schedule alone. A successful encrypted restore counts as a correct answer. The rules are pure
functions in `core/src/core/backup.rs`.

**The app never calls a password safe. It shows zxcvbn's feedback when a password is
guessable.** Fewer than six characters are refused. Anything else is estimated by zxcvbn 3.1.1
(Wheeler, "zxcvbn: Low-Budget Password Strength Estimation", USENIX Security 2016) without its
dictionaries. Its spatial, repeat, sequence, regex and date matchers find keyboard walks, repeats,
sequences, recent years and dates. Its scoring then finds the cheapest way to explain the password
with those matches and counts the rest as brute force. A password estimated at fewer than 10^8
guesses, zxcvbn's score of 2 or less, is accepted with zxcvbn's warning and suggestions, such as
"Straight rows of keys are easy to guess" and "Add another word or two". Above that, the dialog
shows a fixed tip. There is no composition rule, in line with NIST SP 800-63B. The check lives in
`core/src/core/backup/password_check.rs`, and the dialog asks for it as the user types.

Without dictionaries the estimate only ever errs towards more guesses, because a word list can only
add cheaper explanations. A password found guessable is therefore guessable in every language, but
one that is not may still be a single word like `Sommerferien`. That is why the result is never
shown as a meter or a verdict.

Fyles adds one thing. zxcvbn's repeat and sequence matchers are case-sensitive, so the lowercased
password is estimated too and the lower estimate wins. `HalloHALLO123` therefore counts as a
repeat. Lowercasing can only lower the estimate.

**The dictionaries stay out.** zxcvbn's word lists are English, so with them it rated single German
words like `Sommerferien` safe. An English user would find a meter convincing while a German user
relied on a wrong verdict. The lists also added about 1 MB to every native library and 7 MB of
memory once loaded, and lists for more languages would multiply that. A hosted check either sees
the password itself, or, when it compares hashes, finds only exact strings that have already
leaked. The crate cannot run without its dictionaries, so its code is copied instead.

**The copy is vendored with its license.** `fyles_service/zxcvbn_lite` holds zxcvbn-rs 3.1.1 without
the dictionaries and everything that needs them, and its README lists what was removed. The code is
MIT-licensed by Dropbox and by Joshua Holmer, who ported it to Rust. The crate's `LICENSE` carries
both notices, and each source file starts with an SPDX header. cargo-about skips workspace crates,
so `public-site/tools/generate_licenses.py` lists the crate in `VENDORED_RUST_CRATES`. Its notice
reaches the licenses page and the app's license screen like any other Rust dependency's.

**Password managers may keep it.** The fields carry autofill hints, so a system password manager
can offer to save the password and fill it in, cloud-synced or not. That is the user's choice. The
entry has no username, so a manager files every device's backup password under the same app.
Keyboards are still told not to learn what is typed, also while the password is shown.

**A forgotten password is replaced, not recovered.** The check offers "I forgot my password",
which sets a new one. Changing it on purpose first asks for the current one. Both warn that older
backups still need the old password and offer to export a fresh backup right away.

## Consequences

- A user who forgets the password loses every backup made with it. The re-verification schedule
  is the only defence, and it is the reason the schedule exists.
- Restoring a backup from before this change still works without a password. That format
  (`OpenedBackup::LegacyPlain`) is deprecated. Restore logs a warning, and the app tells the user
  to export a new backup. The removal is tracked in `todo/TechnicalTodo.md`.
- Every backup made under one password carries the same salt and key check. Someone holding two
  backup files can tell that they share a password, and learns nothing more about it.
- A damaged salt, Argon2 parameter or key check reads as a wrong password. The key check is the
  only thing compared before decryption, and the password decides it. Damage anywhere else in the
  header or in a chunk fails authentication and reads as a damaged backup.
- An export that fails midway leaves a file without its final chunk, because the Android storage
  framework offers no atomic rename. Restoring such a file reports a damaged backup and changes
  nothing, since a restore replaces the database only after the whole file has authenticated.
- Restoring a legacy backup replaces the device's database with one that has no `backup_secret`
  row, so the next export asks for a password again.
- `BackupData` and `RestoreData` now answer with an outcome rather than a success flag, because
  the client's next step depends on it: choosing a password, or asking for one.
- The `HostController` method that creates the backup file takes the file name, which touches
  every host: desktop, Android (JNI signature), iOS (`fyles.h` and Swift) and the test host.
- A password passes through the local gRPC API in plain text. That API is already restricted to
  the user, and every password inside the service is wrapped in a type whose `Debug` is redacted.
