use std::time::Duration;

/// Channel capacity for internal message passing and exporter buffering.
pub const CHANNEL_CAPACITY: usize = 512;

/// Maximum payload bytes to log when a message payload is not valid JSON.
pub const MAX_NON_JSON_PAYLOAD_LOG_BYTES: usize = 4096;

/// Maximum number of events to batch in a single export flush.
pub const BATCH_MAX_SIZE: usize = 100;

/// Maximum time in milliseconds to wait before flushing a non-empty batch.
pub const BATCH_TIMEOUT_MS: u64 = 10000; // 10 seconds

/// Maximum number of write retries before failing the exporter operation.
pub const MAX_RETRIES: usize = 3;

/// Default JetStream consumer name used for event subscriptions.
pub const JETSTREAM_CONSUMER_NAME: &str = "events-aggregator-consumer";

/// Default JetStream stream name used for publishing and consuming events.
pub const JETSTREAM_STREAM_NAME: &str = "events-stream";

/// Timeout for establishing a NATS connection.
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for NATS request/response operations.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of attempts for retryable operations.
pub const MAX_ATTEMPTS: u32 = 10;

/// Maximum number of backoff retries before giving up.
pub const MAX_BACKOFF_ATTEMPTS: u32 = 6;

/// Base filename for the local event export file.
pub const EVENTS_JSON_FILE: &str = "events.json";
