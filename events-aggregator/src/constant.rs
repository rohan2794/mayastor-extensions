/// Channel capacity for internal message passing and exporter buffering.
pub const CHANNEL_CAPACITY: usize = 512;

/// Maximum number of events to batch in a single export flush.
pub const BATCH_MAX_SIZE: usize = 100;

/// Maximum time in milliseconds to wait before flushing a non-empty batch.
pub const BATCH_TIMEOUT_MS: u64 = 10000; // 10 seconds

/// Maximum number of write retries before failing the exporter operation.
pub const MAX_RETRIES: usize = 3;

/// Default service name used for tracing and application identification.
pub const SERVICE_NAME: &str = "events-aggregator";

/// Default JetStream consumer name used for event subscriptions.
pub const JETSTREAM_CONSUMER_NAME: &str = "events-aggregator-consumer";

/// Default JetStream stream name used for publishing and consuming events.
pub const JETSTREAM_STREAM_NAME: &str = "events-stream";

/// Maximum number of attempts for retryable operations.
pub const MAX_ATTEMPTS: u32 = 10;

/// Maximum number of backoff retries before giving up.
pub const MAX_BACKOFF_ATTEMPTS: u32 = 6;

/// Base filename for the local event export file.
pub const EVENTS_JSON_FILE: &str = "events.json";

/// Filename for the rotated (previous) local event export file.
pub const EVENTS_JSON_ROTATED_FILE: &str = "events.1.json";
