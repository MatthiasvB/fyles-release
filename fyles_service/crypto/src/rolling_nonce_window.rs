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

    fn is_valid(&mut self, nonce: u64) -> Result<(), BitStoreError>;
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
        nonce >= u64::try_from(self.lower_bound).expect(WINDOW_SIZE_CONVERSION_ERROR)
            && nonce
                < self.lower_bound
                    + u64::try_from(self.window.size()).expect(WINDOW_SIZE_CONVERSION_ERROR)
    }

    fn is_valid(&mut self, mut nonce: u64) -> Result<(), BitStoreError> {
        if !self.is_in_bounds(nonce) {
            return Err(BitStoreError::OutOfBoundsAccess {
                tried: nonce,
                min: self.lower_bound,
                max: self.lower_bound + self.window.size() as u64,
            });
        }
        trace!("In bounds: {nonce}");

        let is_valid = self
            .window
            .get_invalidate_at(modulo(nonce, self.window.size()));
        if !is_valid {
            return Err(BitStoreError::RepeatedAccess(nonce));
        }

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

    pub fn validate(&mut self, nonce: u64) -> Result<(), NonceValidationError> {
        self.validator
            .is_valid(if self.is_initiator_session {
                // responder nonces are shifted by half of u64::MAX
                nonce - U64_HALF
            } else {
                nonce
            })
            .map_err(Into::into)
    }
}

impl BitStore for Vec<bool> {
    fn size(&self) -> usize {
        self.len()
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
// TODO: Write tests
