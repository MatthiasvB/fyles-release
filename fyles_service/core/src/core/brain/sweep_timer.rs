//! Cadence for re-poking visible peers.
//!
//! libp2p's mDNS deduplicates: `Event::Discovered` fires only for genuinely new `(peer, addr)`
//! pairs, so a continuously reachable peer announces itself exactly once, ever. This is the missing
//! level trigger.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

use crate::constants::{SWEEP_JITTER_FRACTION, sweep_interval};
use crate::core::brain::client_presence::WakenessLevel;

pub(super) struct SweepTimer {
    interval: Interval,
    level: WakenessLevel,
}

impl SweepTimer {
    pub(super) fn new(level: WakenessLevel) -> Self {
        Self::with_draw(level, clock_draw())
    }

    fn with_draw(level: WakenessLevel, draw: f64) -> Self {
        Self {
            interval: Self::build(level, Delay::Full, draw),
            level,
        }
    }

    pub(super) async fn tick(&mut self) {
        self.interval.tick().await;
    }

    /// Raising the level sweeps at once; lowering waits out a full new period. Rebuilt rather than
    /// reset because an `Interval` keeps its original period.
    pub(super) fn set_level(&mut self, level: WakenessLevel) {
        if level == self.level {
            return;
        }
        let raising = sweep_interval(level) < sweep_interval(self.level);
        let delay = if raising { Delay::None } else { Delay::Full };
        self.interval = Self::build(level, delay, clock_draw());
        self.level = level;
    }

    fn build(level: WakenessLevel, delay: Delay, draw: f64) -> Interval {
        let period = jittered(sweep_interval(level), draw);
        let start = match delay {
            Delay::None => Instant::now(),
            Delay::Full => Instant::now() + period,
        };
        let mut interval = interval_at(start, period);
        // `Burst` would fire a backlog of catch-up ticks when an Android-dozed or iOS-suspended
        // process resumes; we want exactly one sweep.
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        interval
    }
}

enum Delay {
    None,
    Full,
}

/// Not a good random number; it only needs to differ between two devices that booted together, so
/// they do not stay phase-locked and glare on every sweep.
fn clock_draw() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as f64
        / 1_000_000_000.0
}

/// `draw` is in `[0, 1)`, mapped onto `period ± SWEEP_JITTER_FRACTION`.
fn jittered(period: Duration, draw: f64) -> Duration {
    let spread = period.mul_f64(SWEEP_JITTER_FRACTION);
    period + spread.mul_f64(draw * 2.0) - spread
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{SWEEP_INTERVAL_ACTIVE, SWEEP_INTERVAL_IDLE};
    use crate::library::util::duration_ext::DurationExt;
    use futures::FutureExt;

    /// Poll the timer exactly once against the (paused) clock. Deliberately not
    /// `tokio::time::timeout(ZERO, ..)`: that registers a timer of its own, and under a paused
    /// clock with auto-advance the ordering between the two deadlines is not something these tests
    /// should depend on. A single poll is unambiguous.
    fn fired(timer: &mut SweepTimer) -> bool {
        timer.tick().now_or_never().is_some()
    }

    /// Linear in `draw`, so the endpoints are the extremes.
    #[test]
    fn jitter_spans_exactly_the_configured_spread() {
        let period = SWEEP_INTERVAL_IDLE;
        assert_eq!(
            jittered(period, 0.0),
            period.mul_f64(1.0 - SWEEP_JITTER_FRACTION)
        );
        assert_eq!(jittered(period, 0.5), period);
        assert_eq!(
            jittered(period, 1.0),
            period.mul_f64(1.0 + SWEEP_JITTER_FRACTION)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn fires_exactly_once_per_period_and_never_early() {
        let mut timer = SweepTimer::with_draw(WakenessLevel::Active, 0.5);
        for period in 1..=5 {
            tokio::time::advance(SWEEP_INTERVAL_ACTIVE - 1.millis()).await;
            assert!(
                !fired(&mut timer),
                "period {period} fired a millisecond early"
            );

            tokio::time::advance(1.millis()).await;
            assert!(fired(&mut timer), "period {period} never came due");
            assert!(!fired(&mut timer), "period {period} fired twice");
        }
    }

    /// The mobile case: a dozed process wakes hours later and must produce exactly one sweep, not a
    /// burst of catch-up ticks for every period it slept through.
    #[tokio::test(start_paused = true)]
    async fn a_long_suspension_produces_exactly_one_tick() {
        let mut timer = SweepTimer::new(WakenessLevel::Idle);
        tokio::time::advance(SWEEP_INTERVAL_IDLE * 50).await;

        assert!(fired(&mut timer));
        assert!(
            !fired(&mut timer),
            "MissedTickBehavior::Delay is not in effect — the backlog burst through"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn raising_the_level_fires_immediately() {
        let mut timer = SweepTimer::new(WakenessLevel::Idle);
        timer.set_level(WakenessLevel::Active);
        assert!(
            fired(&mut timer),
            "opening the UI should sweep at once, not wait out a period"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lowering_the_level_does_not_fire_early() {
        let mut timer = SweepTimer::new(WakenessLevel::Active);
        timer.set_level(WakenessLevel::Idle);
        tokio::time::advance(SWEEP_INTERVAL_ACTIVE * 2).await;
        assert!(
            !fired(&mut timer),
            "backgrounding the app should not itself trigger a sweep"
        );
    }

    /// A heartbeat arrives every 10s and recomputes the level, so a redundant `set_level` is the
    /// common case. Were it to restart the countdown, a steady heartbeat would postpone the sweep
    /// indefinitely.
    #[tokio::test(start_paused = true)]
    async fn setting_the_same_level_leaves_the_schedule_alone() {
        let mut timer = SweepTimer::with_draw(WakenessLevel::Active, 0.5);
        tokio::time::advance(SWEEP_INTERVAL_ACTIVE - 1.millis()).await;

        timer.set_level(WakenessLevel::Active);

        assert!(!fired(&mut timer), "fired a millisecond early");
        tokio::time::advance(1.millis()).await;
        assert!(fired(&mut timer), "the countdown was restarted");
    }
}
