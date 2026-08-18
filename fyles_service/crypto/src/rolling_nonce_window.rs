use tap::Pipe;
use thiserror::Error;
use tracing::trace;

/// The median of all u64
const U64_HALF: u64 = u64::MAX / 2u64;
const WINDOW_SIZE_CONVERSION_ERROR: &str = "Not running on platforms where usize is larger than 64 bits and even then window sizes will be way smaller";

type NonceSize = u64;

fn modulo(nonce: NonceSize, window_size: usize) -> usize {
    (nonce % u64::try_from(window_size).expect(WINDOW_SIZE_CONVERSION_ERROR))
        .pipe(usize::try_from)
        .expect("Anything modulo usize fitting into usize")
}

pub trait BitStore {
    /// Return the amount of bits that fit into this bit store
    fn size(&self) -> usize;

    /// Return the truth value located at `index` without changing it
    fn get_at(&self, index: usize) -> bool;

    /// Return the truth value located at `index` while setting it to `false` for future access
    fn get_invalidate_at(&mut self, index: usize) -> bool;

    /// Ensure the truth value at `index` is `true` and return whether the value needed to
    /// be changed
    fn validate_at(&mut self, index: usize) -> bool;
}

#[derive(Error, Debug)]
pub enum BitStoreError {
    #[error("Tried to validate a nonce that was outside the currently valid buffer")]
    OutOfBoundsAccess { tried: u64, min: u64, max: u64 },
    #[error("Tried to invalidate the same nonce more than once: {0}")]
    RepeatedAccess(u64),
}

pub trait NonceValidator {
    fn is_in_bounds(&self, nonce: u64) -> bool;

    /// Whether `nonce` would be accepted right now, without recording it as seen.
    fn is_acceptable(&self, nonce: u64) -> Result<(), BitStoreError>;

    /// Record `nonce` as seen, so a later payload carrying it is rejected as a replay.
    fn consume(&mut self, nonce: u64) -> Result<(), BitStoreError>;
}

/// The idea of the rolling nonce window is to allocate a limited size buffer to track
/// the validity of a (effectively) unlimited amount of nonces. The assumption is that nonces
/// are counted up one by one, never leaving one out. Due to race conditions, they may not arrive
/// in order, so there is a `window` of nonces that can be accepted at any time. As we see more
/// and more nonces being used, that window "rools" / shifts forward, changing the nonces we can
/// accept. This is done by tracking the lower bound of acceptable nonces (the first index that
/// leads to a still valid nonce that has only invalid nonces before it). So, a nonce `i` is considered
/// valid if `i - lower_bound` is in range and a still valid index in the `window`.
pub struct RollingNonceWindow<T: BitStore = Vec<bool>> {
    lower_bound: u64,
    window: T,
}

impl<T: BitStore> NonceValidator for RollingNonceWindow<T> {
    fn is_in_bounds(&self, nonce: u64) -> bool {
        nonce >= self.lower_bound
            && nonce
                < self.lower_bound
                    + u64::try_from(self.window.size()).expect(WINDOW_SIZE_CONVERSION_ERROR)
    }

    fn is_acceptable(&self, nonce: u64) -> Result<(), BitStoreError> {
        if !self.is_in_bounds(nonce) {
            return Err(BitStoreError::OutOfBoundsAccess {
                tried: nonce,
                min: self.lower_bound,
                max: self.lower_bound + self.window.size() as u64,
            });
        }
        if !self.window.get_at(modulo(nonce, self.window.size())) {
            return Err(BitStoreError::RepeatedAccess(nonce));
        }
        Ok(())
    }

    fn consume(&mut self, mut nonce: u64) -> Result<(), BitStoreError> {
        self.is_acceptable(nonce)?;
        trace!("In bounds: {nonce}");

        self.window
            .get_invalidate_at(modulo(nonce, self.window.size()));

        if nonce == self.lower_bound {
            // In a perfect world where all nonces arrive in order, we could just increase the lower
            // bound by one and revalidate a single nonce slot. However, nonces may arrive out of order,
            // so sometimes we can not revalidate at all and other times we must validate entire ranges.
            while self.window.validate_at(modulo(nonce, self.window.size())) {
                nonce += 1;
            }

            self.lower_bound = nonce;
        }

        Ok(())
    }
}

pub struct NonceGeneratorValidator<T: NonceValidator = RollingNonceWindow> {
    // encryption scheme we use uses 96 bit nonces. We use 64 of those bits and split
    // them between initiator and responder, meaning each side has effectively 63 bits of nonce space.
    used_nonce_counter: u64,
    is_initiator_session: bool,
    validator: T,
}

#[derive(Error, Debug)]
pub enum NonceGenerationError {
    #[error("Nonce space exhausted: {space_bits} bits")]
    NonceSpaceExhausted { space_bits: usize },
}

#[derive(Error, Debug)]
pub enum NonceValidationError {
    #[error("Nonce was invalid or could not be validated: {0}")]
    NonceValidationError(#[from] BitStoreError),
    /// The nonce sits on our own half of the space, so it was produced by a session with
    /// the same initiator/responder role as us — i.e. it belongs to the opposite session of
    /// a pair, and this is simply the wrong session to decrypt this payload with.
    #[error("Nonce belongs to a session with the inverse role; wrong session for this payload")]
    WrongSessionForDecryption,
}

impl<T: NonceValidator> NonceGeneratorValidator<T> {
    pub fn get_next_send_nonce(&mut self) -> Result<u64, NonceGenerationError> {
        if self.used_nonce_counter == U64_HALF {
            Err(NonceGenerationError::NonceSpaceExhausted { space_bits: 63 })
        } else {
            let nonce = self.used_nonce_counter;
            self.used_nonce_counter += 1;
            if self.is_initiator_session {
                Ok(nonce)
            } else {
                Ok(nonce + U64_HALF)
            }
        }
    }

    /// Whether `nonce` may be decrypted, translated into this validator's space for
    /// [`Self::commit`]. Records nothing: nonces are sent in the clear and run consecutively, so
    /// recording here would let an injected packet spend the slot of a message yet to arrive.
    pub fn check(&self, nonce: u64) -> Result<u64, NonceValidationError> {
        // The nonce's half encodes the sender's role: initiators draw from [0, U64_HALF),
        // responders from [U64_HALF, u64::MAX). A session only ever decrypts payloads from
        // the *opposite* role, so a nonce on our own half means this is the wrong session for
        // this payload — a recoverable "try another session", not an error to blow up on.
        let local_nonce = if self.is_initiator_session {
            nonce
                .checked_sub(U64_HALF)
                .ok_or(NonceValidationError::WrongSessionForDecryption)?
        } else if nonce < U64_HALF {
            nonce
        } else {
            return Err(NonceValidationError::WrongSessionForDecryption);
        };
        self.validator.is_acceptable(local_nonce)?;
        Ok(local_nonce)
    }

    /// Spend the slot for an authenticated payload, taking the nonce [`Self::check`] returned.
    pub fn commit(&mut self, local_nonce: u64) -> Result<(), NonceValidationError> {
        self.validator.consume(local_nonce).map_err(Into::into)
    }
}

impl BitStore for Vec<bool> {
    fn size(&self) -> usize {
        self.len()
    }

    fn get_at(&self, index: usize) -> bool {
        self.get(index).copied().unwrap_or(false)
    }

    fn get_invalidate_at(&mut self, index: usize) -> bool {
        if let Some(validity) = self.get_mut(index) {
            let valid = *validity;
            *validity = false;
            valid
        } else {
            false
        }
    }

    fn validate_at(&mut self, index: usize) -> bool {
        if let Some(validity) = self.get_mut(index) {
            if *validity {
                false
            } else {
                *validity = true;
                true
            }
        } else {
            false
        }
    }
}

fn new_rolling_nonce_window() -> RollingNonceWindow {
    RollingNonceWindow {
        lower_bound: 0,
        window: vec![true; 100],
    }
}

pub fn new_nonce_generator_validator_for_session_initiator() -> NonceGeneratorValidator {
    NonceGeneratorValidator {
        used_nonce_counter: 0,
        is_initiator_session: true,
        validator: new_rolling_nonce_window(),
    }
}

pub fn new_nonce_generator_validator_for_session_responder() -> NonceGeneratorValidator {
    NonceGeneratorValidator {
        used_nonce_counter: 0,
        is_initiator_session: false,
        validator: new_rolling_nonce_window(),
    }
}

// TODO: Implement based on packed value (-> bitvec?!)

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matched_roles_validate_each_others_nonces() {
        let mut initiator = new_nonce_generator_validator_for_session_initiator();
        let mut responder = new_nonce_generator_validator_for_session_responder();
        let init_nonce = initiator.get_next_send_nonce().unwrap();
        let resp_nonce = responder.get_next_send_nonce().unwrap();
        assert!(init_nonce < U64_HALF, "initiator draws from the lower half");
        assert!(
            resp_nonce >= U64_HALF,
            "responder draws from the upper half"
        );
        assert!(responder.check(init_nonce).is_ok());
        assert!(initiator.check(resp_nonce).is_ok());
    }

    #[test]
    fn checking_a_nonce_does_not_spend_it() {
        let mut initiator = new_nonce_generator_validator_for_session_initiator();
        let mut responder = new_nonce_generator_validator_for_session_responder();
        // The second nonce, so the lower bound cannot roll past it while the first is unreceived.
        let _first = initiator.get_next_send_nonce().unwrap();
        let second = initiator.get_next_send_nonce().unwrap();

        for _ in 0..3 {
            assert!(
                responder.check(second).is_ok(),
                "a check must be repeatable — nothing was received yet"
            );
        }

        let local = responder.check(second).unwrap();
        responder.commit(local).unwrap();
        assert!(
            matches!(
                responder.check(second),
                Err(NonceValidationError::NonceValidationError(
                    BitStoreError::RepeatedAccess(_)
                ))
            ),
            "once committed, the same nonce is a replay"
        );
    }

    #[test]
    fn committing_out_of_order_still_rolls_the_window() {
        let mut initiator = new_nonce_generator_validator_for_session_initiator();
        let mut responder = new_nonce_generator_validator_for_session_responder();
        let first = initiator.get_next_send_nonce().unwrap();
        let second = initiator.get_next_send_nonce().unwrap();

        let local_second = responder.check(second).unwrap();
        responder.commit(local_second).unwrap();
        let local_first = responder.check(first).unwrap();
        responder.commit(local_first).unwrap();

        // Refused as out of bounds rather than as replays, the window having rolled past both.
        for nonce in [first, second] {
            assert!(
                responder.check(nonce).is_err(),
                "both nonces stay spent after the window rolls"
            );
        }
    }

    #[test]
    fn same_role_nonce_is_reported_as_wrong_session_not_paniced() {
        // A nonce from a session with our own role means the payload belongs to the opposite
        // session of the pair. This must be a recoverable error (the decrypt loop skips it and
        // tries the next candidate), never the `nonce - U64_HALF` underflow panic.
        let initiator = new_nonce_generator_validator_for_session_initiator();
        let responder = new_nonce_generator_validator_for_session_responder();
        assert!(matches!(
            initiator.check(5),
            Err(NonceValidationError::WrongSessionForDecryption)
        ));
        assert!(matches!(
            responder.check(U64_HALF + 5),
            Err(NonceValidationError::WrongSessionForDecryption)
        ));
    }
}
