use std::time::Duration;

pub trait DurationExt {
    fn seconds(&self) -> Duration;

    fn millis(&self) -> Duration;

    fn minutes(&self) -> Duration;

    fn hours(&self) -> Duration;
}

impl DurationExt for u64 {
    fn seconds(&self) -> Duration {
        Duration::from_secs(*self)
    }

    fn millis(&self) -> Duration {
        Duration::from_millis(*self)
    }

    fn minutes(&self) -> Duration {
        Duration::from_secs(*self * 60)
    }

    fn hours(&self) -> Duration {
        Duration::from_secs(*self * 60 * 60)
    }
}
