use std::cmp;

use zeroize::Zeroizing;
pub use zxcvbn_lite::feedback::{Suggestion as PasswordSuggestion, Warning as PasswordWarning};
use zxcvbn_lite::{Entropy, zxcvbn};

use super::Password;

pub(crate) const MIN_PASSWORD_CHARS: usize = 6;

/// Without word lists the estimate only ever errs towards more guesses, so a password it finds
/// guessable is guessable in every language, and one it does not is not thereby safe.
#[derive(Debug, Clone, PartialEq)]
pub enum PasswordCheck {
    TooShort,
    Guessable {
        warning: Option<PasswordWarning>,
        suggestions: Vec<PasswordSuggestion>,
    },
    NoWeaknessFound,
}

pub(super) fn meets_minimum(password: &Password) -> bool {
    password.as_str().chars().count() >= MIN_PASSWORD_CHARS
}

/// zxcvbn's repeat and sequence matchers are case-sensitive, so the lowercased password is
/// estimated too and the lower estimate wins.
pub fn check_password(password: &Password) -> PasswordCheck {
    if !meets_minimum(password) {
        return PasswordCheck::TooShort;
    }
    let lowercased = Zeroizing::new(password.as_str().to_lowercase());
    let estimate = cmp::min_by_key(
        zxcvbn(password.as_str()),
        zxcvbn(&lowercased),
        Entropy::guesses,
    );
    match estimate.feedback() {
        None => PasswordCheck::NoWeaknessFound,
        Some(feedback) => PasswordCheck::Guessable {
            warning: feedback.warning(),
            suggestions: feedback.suggestions().to_vec(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::PasswordSuggestion::AddAnotherWordOrTwo;
    use super::PasswordWarning::{
        DatesAreOftenEasyToGuess, RepeatsLikeAaaAreEasyToGuess,
        RepeatsLikeAbcAbcAreOnlySlightlyHarderToGuess, SequencesLikeAbcAreEasyToGuess,
        StraightRowsOfKeysAreEasyToGuess,
    };
    use super::*;

    fn check(password: &str) -> PasswordCheck {
        check_password(&Password::new(password.into()))
    }

    fn warning(password: &str) -> Option<PasswordWarning> {
        match check(password) {
            PasswordCheck::Guessable { warning, .. } => warning,
            other => panic!("{password} was not found guessable but {other:?}"),
        }
    }

    #[test]
    fn fewer_than_six_characters_are_refused() {
        assert_eq!(check("abc12"), PasswordCheck::TooShort);
        assert_eq!(check("äöüßé"), PasswordCheck::TooShort);
        assert_ne!(check("äöüßéè"), PasswordCheck::TooShort);
    }

    #[test]
    fn keyboard_walks_are_guessable() {
        assert_eq!(
            warning("qwertyuiop"),
            Some(StraightRowsOfKeysAreEasyToGuess)
        );
        assert_eq!(warning("1qaz2wsx"), Some(StraightRowsOfKeysAreEasyToGuess));
    }

    #[test]
    fn repeats_are_guessable_whatever_their_case() {
        assert_eq!(warning("aaaaaaaaaaaa"), Some(RepeatsLikeAaaAreEasyToGuess));
        assert_eq!(
            warning("hallohallo"),
            Some(RepeatsLikeAbcAbcAreOnlySlightlyHarderToGuess)
        );
        assert_eq!(
            warning("HalloHALLO123"),
            Some(RepeatsLikeAbcAbcAreOnlySlightlyHarderToGuess)
        );
    }

    #[test]
    fn sequences_and_dates_are_guessable() {
        assert_eq!(warning("abcdefghij"), Some(SequencesLikeAbcAreEasyToGuess));
        assert_eq!(warning("19.07.1985"), Some(DatesAreOftenEasyToGuess));
    }

    #[test]
    fn a_short_random_password_asks_for_more() {
        assert_eq!(
            check("k7#pQ2x"),
            PasswordCheck::Guessable {
                warning: None,
                suggestions: vec![AddAnotherWordOrTwo],
            }
        );
    }

    #[test]
    fn long_random_passwords_and_passphrases_show_no_weakness() {
        assert_eq!(check("4719305826"), PasswordCheck::NoWeaknessFound);
        assert_eq!(check("k7#pQ2xLm9"), PasswordCheck::NoWeaknessFound);
        assert_eq!(
            check("korrekt pferd batterie"),
            PasswordCheck::NoWeaknessFound
        );
    }
}
