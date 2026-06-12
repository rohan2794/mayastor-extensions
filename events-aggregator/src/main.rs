mod client;
mod constant;
mod exporter;
use async_nats::jetstream::message::AckKind;
use clap::Parser;
use client::nats_client::{NatsManager, UnifiedMessage};
use constant::CHANNEL_CAPACITY;
use exporter::{dir_size_limit, LogEvent};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};
use url::Url;
use utils::{package_description, tracing_telemetry::TracingTelemetry, version_info_str};

/// Result of message processing.
/// `Ok` = successfully processed, ACK the message
/// `Err(Transient)` = processing failed but might succeed on retry, NAK the message
/// `Err(Permanent)` = unrecoverable failure (e.g., invalid JSON), ACK and discard
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessingResult {
    Success,
    TransientFailure,
    PermanentFailure,
}

#[derive(Parser)]
#[command(name = package_description!(), version = version_info_str!())]
struct CliArgs {
    /// NATS server URL.
    #[arg(long, short, default_value = "nats://mayastor-nats:4222")]
    nats_url: Url,

    /// Enable JetStream subscription mode.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    jetstream_enabled: bool,

    /// Enable Loki scraping mode. When disabled (default), events are persisted to local disk
    /// via --events-dir so they can be retrieved even without Loki.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    loki_enabled: bool,

    /// Events subject filter for NATS subscriptions.
    #[arg(long, default_value = "events.>")]
    subject_filter: String,

    /// Local directory for file-based event export. Required when --loki-enabled is false.
    #[arg(long, short)]
    events_dir: Option<String>,

    /// Maximum total size for local event files under --events-dir, including rotated history.
    /// Supports human-readable units (KiB, MiB, GiB, KB, MB, GB, bytes).
    /// Required when --loki-enabled is false.
    #[arg(long, short, value_parser = dir_size_limit)]
    dir_size_limit: Option<u64>,

    /// Timeout for establishing the NATS connection.
    #[arg(long, default_value = "5s", value_parser = humantime::parse_duration)]
    nats_connection_timeout: Duration,

    /// Timeout for NATS request/response operations.
    #[arg(long, default_value = "10s", value_parser = humantime::parse_duration)]
    nats_request_timeout: Duration,
}

impl CliArgs {
    fn args() -> Self {
        CliArgs::parse()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    TracingTelemetry::builder().init(constant::SERVICE_NAME);
    let cli_args = CliArgs::args();

    let (tx, rx) = mpsc::channel::<UnifiedMessage>(CHANNEL_CAPACITY);

    info!("Event aggregator online and listening for cluster events");
    tracing::info!(
        batch_max_size = exporter::BATCH_MAX_SIZE,
        batch_timeout_ms = exporter::BATCH_TIMEOUT_MS,
        "Exporter batch configuration"
    );

    let nats_mgr = NatsManager::new(
        cli_args.nats_url.as_str(),
        cli_args.nats_connection_timeout,
        cli_args.nats_request_timeout,
    )
    .await
    .map_err(|error| anyhow::anyhow!("Failed to connect to NATS: {error}"))?;
    nats_mgr
        .start_subscribing(
            cli_args.subject_filter,
            cli_args.jetstream_enabled,
            tx,
            cli_args.nats_request_timeout,
        )
        .await?;

    let (tx_exporter, rx_exporter) = mpsc::channel::<LogEvent>(CHANNEL_CAPACITY);

    let exporter_mode = if cli_args.loki_enabled {
        exporter::ExporterMode::Disabled
    } else {
        let events_dir = cli_args.events_dir.clone().ok_or_else(|| {
            anyhow::anyhow!("--events-dir is required when --loki-enabled is false")
        })?;
        let size_limit = cli_args.dir_size_limit.ok_or_else(|| {
            anyhow::anyhow!("--dir-size-limit is required when --loki-enabled is false")
        })?;
        exporter::ExporterMode::File {
            dir: std::path::PathBuf::from(events_dir),
            size_limit,
        }
    };

    tokio::spawn(async move {
        exporter::run_exporter(exporter_mode, rx_exporter).await;
    });

    unified_event_processor(rx, tx_exporter).await;

    Ok(())
}

// Unified event processor that handles both JetStream and Core NATS messages, processes them,
// and sends structured log events to the exporter channel. Manages ACK/NAK logic for JetStream.
async fn unified_event_processor(
    mut rx: mpsc::Receiver<UnifiedMessage>,
    tx_exporter: mpsc::Sender<LogEvent>,
) {
    while let Some(unified_msg) = rx.recv().await {
        match unified_msg {
            UnifiedMessage::JetStream(js_msg) => {
                let subject_str = &js_msg.message.subject;
                let payload_bytes = &js_msg.message.payload;
                let result = process_message(subject_str, payload_bytes, Some(&tx_exporter));
                match result {
                    ProcessingResult::Success => {
                        if let Err(err) = js_msg.ack().await {
                            warn!(
                                subject = %subject_str,
                                error = %err,
                                "Failed to ACK JetStream message"
                            );
                        }
                    }
                    ProcessingResult::TransientFailure => {
                        let backoff_secs = 1 + rand::random::<u64>() % 5;
                        let delay = Duration::from_secs(backoff_secs);
                        if let Err(err) = js_msg.ack_with(AckKind::Nak(Some(delay))).await {
                            warn!(
                                subject = %subject_str,
                                error = %err,
                                "Transient failure: failed to NAK JetStream message for retry"
                            );
                        } else {
                            warn!(
                                subject = %subject_str,
                                "Transient failure; NAKed JetStream message for retry"
                            );
                        }
                    }
                    ProcessingResult::PermanentFailure => {
                        if let Err(err) = js_msg.ack().await {
                            warn!(
                                subject = %subject_str,
                                error = %err,
                                "Permanent failure: failed to ACK JetStream message"
                            );
                        } else {
                            warn!(
                                subject = %subject_str,
                                "Permanent failure; ACKed JetStream message (no retry)"
                            );
                        }
                    }
                }
            }
            UnifiedMessage::Core(core_msg) => {
                // Core NATS is fire-and-forget: no ACK/NAK or retry path.
                let _ = process_message(&core_msg.subject, &core_msg.payload, Some(&tx_exporter));
            }
        }
    }
}

// Process a single message: parse as JSON, log compact single-line output to console,
// and forward to the exporter. Returns a ProcessingResult indicating success or failure kind.
fn process_message(
    subject_str: &str,
    payload_bytes: &[u8],
    tx_exporter: Option<&mpsc::Sender<LogEvent>>,
) -> ProcessingResult {
    let json_payload = match serde_json::from_slice::<Value>(payload_bytes) {
        Ok(v) => v,
        Err(e) => {
            warn!(subject = %subject_str, error = %e, "Received non-JSON event payload; discarding");
            return ProcessingResult::PermanentFailure;
        }
    };

    let log_line = serde_json::json!({
        "fields": {
            "subject": subject_str,
            "payload": json_payload
        },
    });

    let compact = match serde_json::to_string(&log_line) {
        Ok(c) => c,
        Err(e) => {
            warn!(subject = %subject_str, error = %e, "Failed to serialize event for compact JSON output");
            return ProcessingResult::PermanentFailure;
        }
    };

    let Some(tx) = tx_exporter else {
        info!("{compact}");
        return ProcessingResult::Success;
    };

    match tx.try_send(LogEvent {
        line: compact.clone(),
    }) {
        Ok(()) => {
            info!("{compact}");
            ProcessingResult::Success
        }
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            warn!(subject = %subject_str, "Exporter channel full; event not exported");
            ProcessingResult::TransientFailure
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            warn!(subject = %subject_str, "Exporter channel closed; treating as permanent failure");
            ProcessingResult::PermanentFailure
        }
    }
}
