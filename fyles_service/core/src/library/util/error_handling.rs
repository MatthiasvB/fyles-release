use std::sync::Arc;

#[macro_export]
macro_rules! arced_dyn_error {
    ($e:expr) => {
        Arc::new($e) as Arc<dyn std::error::Error + Send + Sync>
    };
}

#[macro_export]
macro_rules! as_arced_dyn_error {
    ($e:expr) => {
        $e as Arc<dyn std::error::Error + Send + Sync>
    };
}

pub trait AutoMapError<T> {
    /// Automatically map the error type to the desired type using the Into trait
    fn auto_map_err(self) -> T;
}

impl<T, U, V> AutoMapError<Result<T, V>> for Result<T, U>
where
    U: Into<V>,
{
    fn auto_map_err(self) -> Result<T, V> {
        self.map_err(|e| e.into())
    }
}

pub trait ToArcedDynError<T>
where
    Self: Sized,
{
    /// Convert the error type to an Arc<dyn Error + Send + Sync>
    fn to_arced_dyn_err(self) -> T;
    /// Convert the error type to an Arc<dyn Error + Send + Sync>
    fn tade(self) -> T {
        self.to_arced_dyn_err()
    }
}

impl<T, U> ToArcedDynError<Result<T, Arc<dyn std::error::Error + Send + Sync>>> for Result<T, U>
where
    U: std::error::Error + Send + Sync + 'static,
{
    fn to_arced_dyn_err(self) -> Result<T, Arc<dyn std::error::Error + Send + Sync>> {
        self.map_err(|e| arced_dyn_error!(e))
    }
}
