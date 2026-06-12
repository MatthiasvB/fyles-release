use crate::crypto::Session as CryptoSession;
use crate::utils::W;
use async_trait::async_trait;
use crypto::{DeSerCryptError, Decrypt, Encrypt, Encrypted, SerCryptError};
use fyles_core::core::domain_models::ContactId;
use serde::de::DeserializeOwned;
use std::sync::Arc;

#[async_trait]
pub trait Session {
    type EncryptError: std::error::Error;
    type DecryptError: std::error::Error;
    type ContactId;

    async fn encrypt<T: Encrypt + DeserializeOwned + Sync>(
        &self,
        payload: &T,
    ) -> Result<Encrypted<T>, Self::EncryptError>;
    async fn decrypt<T: Encrypt + DeserializeOwned + Sync>(
        &self,
        payload: &Encrypted<T>,
    ) -> Result<T, Self::DecryptError>;

    fn get_contact_id(&self) -> Option<Self::ContactId>;
}

#[async_trait]
impl Session for W<Arc<CryptoSession>> {
    type EncryptError = SerCryptError;
    type DecryptError = DeSerCryptError;
    type ContactId = ContactId;

    async fn encrypt<T: Encrypt + DeserializeOwned + Sync>(
        &self,
        payload: &T,
    ) -> Result<Encrypted<T>, Self::EncryptError> {
        payload.encrypt(&mut *self.dencryptor.lock().await)
    }

    async fn decrypt<T: Encrypt + DeserializeOwned + Sync>(
        &self,
        payload: &Encrypted<T>,
    ) -> Result<T, Self::DecryptError> {
        payload.decrypt(&mut *self.dencryptor.lock().await)
    }

    fn get_contact_id(&self) -> Option<Self::ContactId> {
        self.authentication_type.get_contact_id().cloned()
    }
}
