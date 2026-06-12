#[cfg(any(feature = "console", feature = "otel-jaeger"))]
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::{util::SubscriberInitExt, Registry};

pub struct TracingConfig {
    pub with_target: bool,
    pub with_line_number: bool,
    pub max_events_per_span: Option<u32>,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            with_target: true,
            with_line_number: false,
            max_events_per_span: None,
        }
    }
}

/// Returned by `init_tracing`.  
/// On builds that include the `otel-jaeger` feature it holds the
/// `SdkTracerProvider`; otherwise it’s an empty shell.
/// `shutdown()` is always available and does the right thing.
pub struct TraceGuard {
    #[cfg(feature = "otel-jaeger")]
    provider: opentelemetry_sdk::trace::SdkTracerProvider,
}

/// Initialise tracing.
///
/// * `service_name` – how the service appears in Jaeger.
/// * `otel_endpoint` – optional OTLP/HTTP or gRPC endpoint  
///   (e.g. `"http://127.0.0.1:4318"`), **used only** when the
///   `otel-jaeger` feature is compiled in.  
///   If `None`, falls back to the `OTEL_EXPORTER_OTLP_ENDPOINT` env-var
///   or the default `http://127.0.0.1:4318`.
#[allow(unused)]
pub fn init_tracing(
    service_name: String,
    otel_endpoint: Option<&str>,
    config: TracingConfig,
) -> TraceGuard {
    let subscriber = Registry::default();

    #[cfg(any(feature = "console", feature = "otel-jaeger"))]
    let filter = || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // console layer
    #[cfg(feature = "console")]
    let subscriber = {
        use tracing_subscriber::Layer;
        use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

        subscriber.with(
            tracing_subscriber::fmt::layer()
                .with_target(config.with_target)
                .with_line_number(config.with_line_number)
                .with_filter(filter()),
        )
    };

    // Jaeger layer
    #[cfg(feature = "otel-jaeger")]
    let (subscriber, provider_opt) = {
        use opentelemetry::global;
        use opentelemetry::trace::TracerProvider;
        use opentelemetry_otlp::{ExportConfig, Protocol, WithExportConfig};
        use opentelemetry_sdk::trace::Sampler;
        use opentelemetry_sdk::{propagation::TraceContextPropagator, Resource};
        use tracing_subscriber::Layer;
        use tracing_subscriber::filter::{LevelFilter, Targets};
        use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;

        let resource = Resource::builder_empty()
            .with_service_name(service_name)
            .build();

        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(otel_endpoint.unwrap_or("http://127.0.0.1:4318/v1/traces"))
            .build()
            .expect("create OTLP exporter");

        let mut provider_builder = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(Sampler::AlwaysOn)
            .with_resource(resource);

        if let Some(max_events) = config.max_events_per_span {
            provider_builder = provider_builder.with_max_events_per_span(max_events);
        }

        let provider = provider_builder.build();

        global::set_tracer_provider(provider.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());

        let otel_layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer(format!(
                "{}-{}",
                env!("CARGO_CRATE_NAME"),
                env!("CARGO_PKG_VERSION")
            )))
            .with_filter(filter());

        (subscriber.with(otel_layer), provider)
    };

    subscriber.init();

    TraceGuard {
        #[cfg(feature = "otel-jaeger")]
        provider: provider_opt,
    }
}

impl Drop for TraceGuard {
    fn drop(&mut self) {
        #[cfg(feature = "otel-jaeger")]
        {
            self.provider
                .shutdown()
                .ok()
                .expect("Failed to shutdown tracing provider");
        }
    }
}
