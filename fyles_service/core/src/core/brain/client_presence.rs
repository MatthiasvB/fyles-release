//! Client presence, and the [`WakenessLevel`] derived from it.
//!
//! Heartbeats rather than the stream lifecycle, because a half-open connection keeps the stream
//! object alive long after the client is gone. Nothing is tracked per client: whether one is
//! attached or three makes no difference to the cadence.

use std::future::pending;
use std::time::Duration;
use tokio::time::Instant;
use tracing::trace;

use crate::constants::CLIENT_HEARTBEAT_TTL;
use crate::core::brain::GigaBrain;
use crate::core::brain::sweep_timer::SweepTimer;

/// How eagerly we re-poke the peers we can see. Wall-clock values live in [`crate::constants`].
/// Deliberately not part of the [`NetworkNode`](crate::core::p2p::NetworkNode) interface: the
/// network has no business knowing that clients have foregrounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WakenessLevel {
    /// No recent heartbeat.
    #[default]
    Idle,
    /// A client is alive but backgrounded.
    Attached,
    /// A client is alive and in the foreground.
    Active,
}

#[derive(Default)]
pub(super) struct ClientPresence {
    any: Option<Instant>,
    /// Tracked separately from `any` so a backgrounding client decays `Active -> Attached` promptly.
    foreground: Option<Instant>,
}

impl ClientPresence {
    pub(super) fn observe(&mut self, foreground: bool) {
        let now = Instant::now();
        self.any = Some(now);
        if foreground {
            self.foreground = Some(now);
        }
    }

    pub(super) fn level(&self) -> WakenessLevel {
        let now = Instant::now();
        let live = |beat: Option<Instant>| {
            beat.is_some_and(|t| now.saturating_duration_since(t) < CLIENT_HEARTBEAT_TTL)
        };
        if live(self.foreground) {
            WakenessLevel::Active
        } else if live(self.any) {
            WakenessLevel::Attached
        } else {
            WakenessLevel::Idle
        }
    }

    /// When the level next drops a step on its own. `None` at `Idle` — nothing left to decay, so
    /// the watcher parks until a heartbeat arrives.
    pub(super) fn next_decay_in(&self) -> Option<Duration> {
        let now = Instant::now();
        let remaining = |beat: Option<Instant>| {
            beat.map(|t| CLIENT_HEARTBEAT_TTL.saturating_sub(now.saturating_duration_since(t)))
                .filter(|d| !d.is_zero())
        };
        match (remaining(self.foreground), remaining(self.any)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

impl GigaBrain {
    pub(super) async fn observe_client_heartbeat(&self, foreground: bool) {
        self.ephemeral().await.client_presence.observe(foreground);
        self.presence_wakeup.notify_one();
    }

    /// Sole owner of the level: it recomputes on every wake and hands the result to the timer,
    /// which is idempotent, so a redundant heartbeat costs nothing. Runs as a spawned workload,
    /// hence the timer being a local rather than shared state.
    pub(super) async fn watch_presence_decay(&self) {
        let mut sweep = SweepTimer::new(WakenessLevel::default());
        loop {
            // Bound before the select: a scrutinee temporary would hold the ephemeral lock across
            // the park below, which deadlocks every heartbeat.
            let next_decay = self.ephemeral().await.client_presence.next_decay_in();
            tokio::select! {
                _ = sweep.tick() => {
                    trace!("Triggering visible peer sweep");
                    self.network.read().await.trigger_interactions_with_visible_peers().await;
                }
                // The only thing that makes a rise take effect before the decay deadline.
                _ = self.presence_wakeup.notified() => {}
                _ = decay_deadline(next_decay) => {}
            }
            sweep.set_level(self.ephemeral().await.client_presence.level());
        }
    }
}

/// `None` means nothing is left to decay, so this arm must never win — the heartbeat does.
async fn decay_deadline(next_decay: Option<Duration>) {
    match next_decay {
        Some(until_decay) => tokio::time::sleep(until_decay).await,
        None => pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::CLIENT_HEARTBEAT_INTERVAL;

    #[tokio::test(start_paused = true)]
    async fn no_heartbeat_means_idle() {
        let presence = ClientPresence::default();
        assert_eq!(presence.level(), WakenessLevel::Idle);
        assert_eq!(presence.next_decay_in(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_foreground_beat_is_active_and_a_background_beat_is_attached() {
        let mut presence = ClientPresence::default();
        presence.observe(true);
        assert_eq!(presence.level(), WakenessLevel::Active);

        presence.observe(false);
        // The foreground claim is still inside its TTL, so it still counts.
        assert_eq!(presence.level(), WakenessLevel::Active);

        tokio::time::advance(CLIENT_HEARTBEAT_TTL + CLIENT_HEARTBEAT_INTERVAL).await;
        presence.observe(false);
        assert_eq!(
            presence.level(),
            WakenessLevel::Attached,
            "a client that stopped reporting foreground should drop to Attached"
        );
    }

    /// The case a stream-lifecycle-only design cannot see: the client is force-killed, so it never
    /// closes anything, it just stops talking.
    #[tokio::test(start_paused = true)]
    async fn a_silent_client_decays_to_idle() {
        let mut presence = ClientPresence::default();
        presence.observe(true);
        assert_eq!(presence.level(), WakenessLevel::Active);

        tokio::time::advance(CLIENT_HEARTBEAT_TTL + CLIENT_HEARTBEAT_INTERVAL).await;
        assert_eq!(presence.level(), WakenessLevel::Idle);
        assert_eq!(presence.next_decay_in(), None);
    }

    /// Two lost beats must be survivable, or a momentarily busy client would keep flapping the
    /// node's cadence.
    #[tokio::test(start_paused = true)]
    async fn a_single_missed_beat_does_not_drop_the_level() {
        let mut presence = ClientPresence::default();
        presence.observe(true);
        tokio::time::advance(CLIENT_HEARTBEAT_INTERVAL * 2).await;
        assert_eq!(presence.level(), WakenessLevel::Active);
    }

    #[tokio::test(start_paused = true)]
    async fn a_returning_client_restores_the_level_at_once() {
        let mut presence = ClientPresence::default();
        presence.observe(true);
        tokio::time::advance(CLIENT_HEARTBEAT_TTL * 4).await;
        assert_eq!(presence.level(), WakenessLevel::Idle);

        presence.observe(true);
        assert_eq!(presence.level(), WakenessLevel::Active);
    }

    #[tokio::test(start_paused = true)]
    async fn decay_deadline_is_reported_while_a_client_is_present() {
        let mut presence = ClientPresence::default();
        presence.observe(true);
        let due = presence.next_decay_in().expect("a live beat to decay");
        assert!(due <= CLIENT_HEARTBEAT_TTL && !due.is_zero());

        tokio::time::advance(due).await;
        assert_eq!(presence.level(), WakenessLevel::Idle);
    }

    /// The steady state: a beat every interval, all implying the level already in effect. That the
    /// resulting redundant `set_level` leaves the schedule alone is
    /// [`SweepTimer`](crate::core::brain::sweep_timer)'s job, tested there.
    #[tokio::test(start_paused = true)]
    async fn a_steady_beat_holds_the_level_and_silence_drops_it() {
        let mut presence = ClientPresence::default();
        presence.observe(true);
        assert_eq!(presence.level(), WakenessLevel::Active);

        for _ in 0..5 {
            tokio::time::advance(CLIENT_HEARTBEAT_INTERVAL).await;
            presence.observe(true);
            assert_eq!(presence.level(), WakenessLevel::Active);
        }

        tokio::time::advance(CLIENT_HEARTBEAT_TTL * 2).await;
        assert_eq!(presence.level(), WakenessLevel::Idle);
    }
}
