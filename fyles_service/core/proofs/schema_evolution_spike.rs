//! Verifies, against the pinned `autosurgeon` 0.9 / `automerge` 0.7, how a struct
//! hydrates from a document written under a *different* schema. Conclusions:
//!
//! 1. **Adding a field** is only compatible via `#[autosurgeon(missing = "fn")]`.
//!    A bare `Option<T>` field does NOT tolerate an absent key — it errors.
//! 2. **Removing a field** is always safe: unknown keys in the doc are ignored.
//! 3. **Adding an enum variant** is NOT safe: an unknown variant tag fails to
//!    hydrate. Mitigation: a catch-all variant with a custom `hydrate_with`.

use automerge::AutoCommit;
use autosurgeon::{Hydrate, Reconcile, hydrate, reconcile};

fn default_kind() -> String {
    "unknown".to_string()
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
struct EntryV1 {
    name: String,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
struct EntryV2Missing {
    name: String,
    #[autosurgeon(missing = "default_kind")]
    kind: String,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
struct EntryV2Option {
    name: String,
    extra: Option<String>,
}

#[test]
fn added_field_with_missing_attr_hydrates_with_default() {
    let mut doc = AutoCommit::new();
    reconcile(
        &mut doc,
        EntryV1 {
            name: "alice".into(),
        },
    )
    .unwrap();

    let hydrated: EntryV2Missing = hydrate(&doc).unwrap();
    assert_eq!(hydrated.name, "alice");
    assert_eq!(hydrated.kind, default_kind());
}

#[test]
fn added_bare_option_field_fails_on_absent_key() {
    let mut doc = AutoCommit::new();
    reconcile(
        &mut doc,
        EntryV1 {
            name: "alice".into(),
        },
    )
    .unwrap();

    let hydrated: Result<EntryV2Option, _> = hydrate(&doc);
    assert!(
        hydrated.is_err(),
        "bare Option must NOT silently tolerate an absent key"
    );
}

#[test]
fn removed_field_is_ignored() {
    let mut doc = AutoCommit::new();
    reconcile(
        &mut doc,
        EntryV2Missing {
            name: "bob".into(),
            kind: "person".into(),
        },
    )
    .unwrap();

    let hydrated: EntryV1 = hydrate(&doc).unwrap();
    assert_eq!(hydrated.name, "bob");
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
enum ColorV1 {
    Red,
    Blue,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
enum ColorV2 {
    Blue,
    Green,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
struct HolderV1 {
    color: ColorV1,
}

#[derive(Debug, Clone, Reconcile, Hydrate)]
struct HolderV2 {
    color: ColorV2,
}

#[test]
fn unknown_enum_variant_fails_to_hydrate() {
    let mut doc = AutoCommit::new();
    reconcile(
        &mut doc,
        HolderV1 {
            color: ColorV1::Red,
        },
    )
    .unwrap();

    let hydrated: Result<HolderV2, _> = hydrate(&doc);
    assert!(
        hydrated.is_err(),
        "an unknown enum variant tag must fail to hydrate"
    );
}

#[test]
fn known_enum_variant_still_hydrates() {
    let mut doc = AutoCommit::new();
    reconcile(
        &mut doc,
        HolderV1 {
            color: ColorV1::Blue,
        },
    )
    .unwrap();

    let hydrated: HolderV2 = hydrate(&doc).unwrap();
    assert!(matches!(hydrated.color, ColorV2::Blue));
}
