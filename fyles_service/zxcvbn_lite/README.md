# zxcvbn_lite

[zxcvbn-rs](https://github.com/shssoichiro/zxcvbn-rs) 3.1.1, a Rust port of Dropbox's
[zxcvbn](https://github.com/dropbox/zxcvbn), without its dictionaries. Both are MIT-licensed, and
`LICENSE` carries both copyright notices. Fyles uses it to estimate backup passwords
([ADR-0012](../../docs/architecture/adr/0012-password-encrypted-backups.md)).

Removed from upstream:

- the frequency lists and everything that needs them: the dictionary, reversed-dictionary and
  l33t matchers, their pattern, scoring and feedback, and the `user_inputs` parameter,
- crack-time estimates, the calculation time, and the `ser`, `builder` and WebAssembly support,
- the upstream tests that depend on any of these.

Unchanged: the spatial, repeat, sequence, regex and date matchers, the keyboard graphs, the
guess estimation, the minimum-guesses search, the score thresholds and the feedback for the
remaining patterns. `clippy::question_mark` is allowed so that upstream code stays as it was.
