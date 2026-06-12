use std::time::{SystemTime, SystemTimeError};

/// Returns the current time as milliseconds since the Unix epoch.
///
/// The result is `i64` because SQLite `INTEGER` is signed 64-bit. This is safe:
/// current epoch millis (~1.7 × 10¹²) fits within `i64::MAX` (~9.2 × 10¹⁸),
/// leaving headroom for roughly 292 million years.
///
/// # Errors
///
/// Returns `SystemTimeError` if the system clock is set before the Unix epoch.
pub fn unix_epoch_millis() -> Result<i64, SystemTimeError> {
    let millis_u128 = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_millis();
    Ok(millis_u128 as i64)
}
