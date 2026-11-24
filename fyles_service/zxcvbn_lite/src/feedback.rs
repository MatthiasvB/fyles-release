// SPDX-License-Identifier: MIT
// Copyright (c) 2012-2016 Dan Wheeler and Dropbox, Inc.
// Copyright (c) 2016 Joshua Holmer

//! Contains structs and methods related to generating feedback strings
//! for providing help for the user to generate stronger passwords.

use itertools::Itertools;

use crate::matching::patterns::*;
use crate::matching::Match;
use crate::scoring::Score;
use std::fmt;

/// A warning explains what's wrong with the password.
#[derive(Debug, Copy, Clone, PartialEq)]
#[allow(missing_docs)]
pub enum Warning {
    StraightRowsOfKeysAreEasyToGuess,
    ShortKeyboardPatternsAreEasyToGuess,
    RepeatsLikeAaaAreEasyToGuess,
    RepeatsLikeAbcAbcAreOnlySlightlyHarderToGuess,
    SequencesLikeAbcAreEasyToGuess,
    RecentYearsAreEasyToGuess,
    DatesAreOftenEasyToGuess,
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Warning::StraightRowsOfKeysAreEasyToGuess => {
                write!(f, "Straight rows of keys are easy to guess.")
            }
            Warning::ShortKeyboardPatternsAreEasyToGuess => {
                write!(f, "Short keyboard patterns are easy to guess.")
            }
            Warning::RepeatsLikeAaaAreEasyToGuess => {
                write!(f, "Repeats like \"aaa\" are easy to guess.")
            }
            Warning::RepeatsLikeAbcAbcAreOnlySlightlyHarderToGuess => write!(
                f,
                "Repeats like \"abcabcabc\" are only slightly harder to guess than \"abc\"."
            ),
            Warning::SequencesLikeAbcAreEasyToGuess => {
                write!(f, "Sequences like abc or 6543 are easy to guess.")
            }
            Warning::RecentYearsAreEasyToGuess => write!(f, "Recent years are easy to guess."),
            Warning::DatesAreOftenEasyToGuess => write!(f, "Dates are often easy to guess."),
        }
    }
}

/// A suggestion helps to choose a better password.
#[derive(Debug, Copy, Clone, PartialEq)]
#[allow(missing_docs)]
pub enum Suggestion {
    UseAFewWordsAvoidCommonPhrases,
    NoNeedForSymbolsDigitsOrUppercaseLetters,
    AddAnotherWordOrTwo,
    UseALongerKeyboardPatternWithMoreTurns,
    AvoidRepeatedWordsAndCharacters,
    AvoidSequences,
    AvoidRecentYears,
    AvoidYearsThatAreAssociatedWithYou,
    AvoidDatesAndYearsThatAreAssociatedWithYou,
}

impl fmt::Display for Suggestion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Suggestion::UseAFewWordsAvoidCommonPhrases => {
                write!(f, "Use a few words, avoid common phrases.")
            }
            Suggestion::NoNeedForSymbolsDigitsOrUppercaseLetters => {
                write!(f, "No need for symbols, digits, or uppercase letters.")
            }
            Suggestion::AddAnotherWordOrTwo => {
                write!(f, "Add another word or two. Uncommon words are better.")
            }
            Suggestion::UseALongerKeyboardPatternWithMoreTurns => {
                write!(f, "Use a longer keyboard pattern with more turns.")
            }
            Suggestion::AvoidRepeatedWordsAndCharacters => {
                write!(f, "Avoid repeated words and characters.")
            }
            Suggestion::AvoidSequences => write!(f, "Avoid sequences."),
            Suggestion::AvoidRecentYears => write!(f, "Avoid recent years."),
            Suggestion::AvoidYearsThatAreAssociatedWithYou => {
                write!(f, "Avoid years that are associated with you.")
            }
            Suggestion::AvoidDatesAndYearsThatAreAssociatedWithYou => {
                write!(f, "Avoid dates and years that are associated with you.")
            }
        }
    }
}

/// Verbal feedback to help choose better passwords
#[derive(Debug, PartialEq, Clone, Default)]
pub struct Feedback {
    /// Explains what's wrong, e.g. "This is a top-10 common password". Not always set.
    warning: Option<Warning>,
    /// A possibly-empty list of suggestions to help choose a less guessable password.
    /// E.g. "Add another word or two".
    suggestions: Vec<Suggestion>,
}

impl Feedback {
    /// Explains what's wrong, e.g. "This is a top-10 common password". Not always set.
    pub fn warning(&self) -> Option<Warning> {
        self.warning
    }

    /// A possibly-empty list of suggestions to help choose a less guessable password.
    /// E.g. "Add another word or two".
    pub fn suggestions(&self) -> &[Suggestion] {
        &self.suggestions
    }
}

impl fmt::Display for Feedback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(warning) = self.warning {
            write!(f, "{} ", warning)?;
        }
        write!(f, "{}", self.suggestions.iter().join(" "))?;

        Ok(())
    }
}

pub(crate) fn get_feedback(score: Score, sequence: &[Match]) -> Option<Feedback> {
    if sequence.is_empty() {
        // default feedback
        return Some(Feedback {
            warning: None,
            suggestions: vec![
                Suggestion::UseAFewWordsAvoidCommonPhrases,
                Suggestion::NoNeedForSymbolsDigitsOrUppercaseLetters,
            ],
        });
    }
    if score >= Score::Three {
        return None;
    }

    let longest_match = sequence
        .iter()
        .max_by_key(|x| x.token.chars().count())
        .unwrap();
    let mut feedback = get_match_feedback(longest_match);
    let extra_feedback = Suggestion::AddAnotherWordOrTwo;

    feedback.suggestions.insert(0, extra_feedback);
    Some(feedback)
}

fn get_match_feedback(cur_match: &Match) -> Feedback {
    match cur_match.pattern {
        MatchPattern::Spatial(ref pattern) => Feedback {
            warning: Some(if pattern.turns == 1 {
                Warning::StraightRowsOfKeysAreEasyToGuess
            } else {
                Warning::ShortKeyboardPatternsAreEasyToGuess
            }),
            suggestions: vec![Suggestion::UseALongerKeyboardPatternWithMoreTurns],
        },
        MatchPattern::Repeat(ref pattern) => Feedback {
            warning: Some(if pattern.base_token.chars().count() == 1 {
                Warning::RepeatsLikeAaaAreEasyToGuess
            } else {
                Warning::RepeatsLikeAbcAbcAreOnlySlightlyHarderToGuess
            }),
            suggestions: vec![Suggestion::AvoidRepeatedWordsAndCharacters],
        },
        MatchPattern::Sequence(_) => Feedback {
            warning: Some(Warning::SequencesLikeAbcAreEasyToGuess),
            suggestions: vec![Suggestion::AvoidSequences],
        },
        MatchPattern::Regex(ref pattern) => {
            if pattern.regex_name == "recent_year" {
                Feedback {
                    warning: Some(Warning::RecentYearsAreEasyToGuess),
                    suggestions: vec![
                        Suggestion::AvoidRecentYears,
                        Suggestion::AvoidYearsThatAreAssociatedWithYou,
                    ],
                }
            } else {
                Feedback::default()
            }
        }
        MatchPattern::Date(_) => Feedback {
            warning: Some(Warning::DatesAreOftenEasyToGuess),
            suggestions: vec![Suggestion::AvoidDatesAndYearsThatAreAssociatedWithYou],
        },
        _ => Feedback {
            warning: None,
            suggestions: vec![],
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feedback_display() {
        let feedback = Feedback {
            warning: Some(Warning::SequencesLikeAbcAreEasyToGuess),
            suggestions: vec![Suggestion::UseAFewWordsAvoidCommonPhrases],
        };
        assert_eq!(
            format!("{}", feedback),
            "Sequences like abc or 6543 are easy to guess. Use a few words, avoid common phrases."
        );
    }
}
