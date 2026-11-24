// SPDX-License-Identifier: MIT
// Copyright (c) 2012-2016 Dan Wheeler and Dropbox, Inc.
// Copyright (c) 2016 Joshua Holmer
//
// zxcvbn 3.1.1 without its dictionaries. See README.md for what was changed.

#![recursion_limit = "128"]
#![warn(missing_docs)]
#![allow(clippy::question_mark)]
//! zxcvbn's password guess estimation, limited to the matchers that need no word lists.

#[macro_use]
extern crate lazy_static;

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

pub use scoring::Score;

pub use crate::matching::Match;

mod adjacency_graphs;
pub mod feedback;
/// Defines structures for matches found in a password
pub mod matching;
mod scoring;

/// Contains the results of an entropy calculation
#[derive(Debug, PartialEq, Clone)]
pub struct Entropy {
    /// Estimated guesses needed to crack the password
    guesses: u64,
    /// Order of magnitude of `guesses`
    guesses_log10: f64,
    /// Overall strength score from 0-4.
    /// Any score less than 3 should be considered too weak.
    score: Score,
    /// Verbal feedback to help choose better passwords. Set when `score` <= 2.
    feedback: Option<feedback::Feedback>,
    /// The list of patterns the guess calculation was based on
    sequence: Vec<Match>,
}

impl Entropy {
    /// The estimated number of guesses needed to crack the password.
    pub fn guesses(&self) -> u64 {
        self.guesses
    }

    /// The order of magnitude of `guesses`.
    pub fn guesses_log10(&self) -> f64 {
        self.guesses_log10
    }

    /// Overall strength score from 0-4.
    /// Any score less than 3 should be considered too weak.
    pub fn score(&self) -> Score {
        self.score
    }

    /// Feedback to help choose better passwords. Set when `score` <= 2.
    pub fn feedback(&self) -> Option<&feedback::Feedback> {
        self.feedback.as_ref()
    }

    /// The list of patterns the guess calculation was based on
    pub fn sequence(&self) -> &[Match] {
        &self.sequence
    }
}

/// Takes a password string and calculates the strength of the password
/// based on entropy, using a number of different factors.
pub fn zxcvbn(password: &str) -> Entropy {
    if password.is_empty() {
        return Entropy {
            guesses: 0,
            guesses_log10: f64::NEG_INFINITY,
            score: Score::Zero,
            feedback: feedback::get_feedback(Score::Zero, &[]),
            sequence: Vec::default(),
        };
    }

    // Only evaluate the first 100 characters of the input.
    // This prevents potential DoS attacks from sending extremely long input strings.
    let password = password.chars().take(100).collect::<String>();

    let matches = matching::omnimatch(&password);
    let result = scoring::most_guessable_match_sequence(&password, &matches, false);
    let score = calculate_score(result.guesses);
    let feedback = feedback::get_feedback(score, &result.sequence);

    Entropy {
        guesses: result.guesses,
        guesses_log10: result.guesses_log10,
        score,
        feedback,
        sequence: result.sequence,
    }
}

fn calculate_score(guesses: u64) -> Score {
    const DELTA: u64 = 5;
    if guesses < 1_000 + DELTA {
        Score::Zero
    } else if guesses < 1_000_000 + DELTA {
        Score::One
    } else if guesses < 100_000_000 + DELTA {
        Score::Two
    } else if guesses < 10_000_000_000 + DELTA {
        Score::Three
    } else {
        Score::Four
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use quickcheck::TestResult;

    quickcheck! {
        fn test_zxcvbn_doesnt_panic(password: String) -> TestResult {
            zxcvbn(&password);
            TestResult::from_bool(true)
        }
    }

    #[test]
    fn test_zxcvbn_empty() {
        let password = "";
        let entropy = zxcvbn(password);
        assert_eq!(entropy.score, Score::Zero);
        assert_eq!(entropy.guesses, 0);
        assert_eq!(entropy.guesses_log10, f64::NEG_INFINITY);
        assert_eq!(entropy.sequence, Vec::default());
    }

    #[test]
    fn test_zxcvbn_unicode() {
        let password = "𐰊𐰂𐰄𐰀𐰁";
        let entropy = zxcvbn(password);
        assert_eq!(entropy.score, Score::One);
    }

    #[test]
    fn test_issue_13() {
        let password = "Imaginative-Say-Shoulder-Dish-0";
        let entropy = zxcvbn(password);
        assert_eq!(entropy.score, Score::Four);
    }

    #[test]
    fn test_overflow_safety() {
        let password = "!QASW@#EDFR$%TGHY^&UJKI*(OL";
        let entropy = zxcvbn(password);
        assert_eq!(entropy.guesses, u64::MAX);
        assert_eq!(entropy.score, Score::Four);
    }

    #[test]
    fn test_unicode_mb() {
        let password = "08märz2010";
        let entropy = zxcvbn(password);
        assert_eq!(entropy.guesses, 100010000);
        assert_eq!(entropy.score, Score::Three);
    }
}
