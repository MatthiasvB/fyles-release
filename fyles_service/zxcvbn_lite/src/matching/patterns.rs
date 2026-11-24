// SPDX-License-Identifier: MIT
// Copyright (c) 2012-2016 Dan Wheeler and Dropbox, Inc.
// Copyright (c) 2016 Joshua Holmer

use crate::matching::Match;

/// Pattern type used to detect a match
#[derive(Debug, Clone, PartialEq, Default)]
pub enum MatchPattern {
    /// A match based on keys being close to one another on the keyboard
    Spatial(SpatialPattern),
    /// A match based on repeating patterns
    Repeat(RepeatPattern),
    /// A match based on sequences of characters, e.g. "abcd"
    Sequence(SequencePattern),
    /// A match based on one of the regex patterns used in zxcvbn.
    Regex(RegexPattern),
    /// A match based on date patterns
    Date(DatePattern),
    /// A match based on bruteforce attempting to guess a password
    #[default]
    BruteForce,
}

impl MatchPattern {
    #[cfg(test)]
    pub(crate) fn variant(&self) -> &str {
        match *self {
            MatchPattern::Spatial(_) => "spatial",
            MatchPattern::Repeat(_) => "repeat",
            MatchPattern::Sequence(_) => "sequence",
            MatchPattern::Regex(_) => "regex",
            MatchPattern::Date(_) => "date",
            MatchPattern::BruteForce => "bruteforce",
        }
    }
}

/// A match based on keys being close to one another on the keyboard
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SpatialPattern {
    /// Name of the graph for which a spatial match has been found.
    pub graph: String,
    /// Number of turns in the matched spatial pattern.
    pub turns: usize,
    /// Number of shifts in the matched spatial pattern.
    pub shifted_count: usize,
}

/// A match based on repeating patterns
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RepeatPattern {
    /// Base token that repeats in the matched pattern.
    pub base_token: String,
    /// Matches for the repeating token.
    pub base_matches: Vec<Match>,
    /// Estimated number of tries for guessing the repeating token.
    pub base_guesses: u64,
    /// Number of repetitions in the matched pattern.
    pub repeat_count: usize,
}

/// A match based on sequences of characters, e.g. "abcd"
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SequencePattern {
    /// Name of the sequence that was matched.
    pub sequence_name: String,
    /// Size of the sequence that was matched.
    pub sequence_space: u8,
    /// Whether the matched sequence is ascending.
    pub ascending: bool,
}

/// A match based on one of the regex patterns used in zxcvbn.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RegexPattern {
    /// Name of the regular expression that was matched.
    pub regex_name: String,
    /// Matches of the regular expression.
    pub regex_match: Vec<String>,
}

/// A match based on date patterns
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DatePattern {
    /// Separator of a date that was matched.
    pub separator: String,
    /// Year that was matched.
    pub year: i32,
    /// Month that was matched.
    pub month: i8,
    /// Day that was matched.
    pub day: i8,
}
