use derive_more::{AsRef, Deref, From};

#[derive(Debug, Clone, Deref, AsRef, From)]
pub struct SettingsBlob(pub Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, Deref, AsRef, From)]
pub struct SettingsView(pub Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, Deref, AsRef, From)]
pub struct SettingsMutation(pub Vec<u8>);
