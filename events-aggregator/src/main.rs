mod client;
mod exporter;
use async_nats::jetstream::message::AckKind;
use clap::Parser;
use client::nats_client::{NatsManager, UnifiedMessage};
use exporter::{parse_dir_size_limit, LogEvent};
use serde_json::Value;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;
use utils::{package_description, version_info_str};

/// Result of message processing.
/// `Ok` = successfully processed, ACK the message
/// `Err(Transient)` = processing failed but might succeed on retry, NAK the message
/// `Err(Permanent)` = unrecoverable failure (e.g., invalid JSON), ACK and discard
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum ProcessingResult {
    Success,
    TransientFailure,
    PermanentFailure,
}

/// channel capacity for both internal unified message channel and exporter channel;
const CHANNEL_CAPACITY: usize = 512;

// Command-line arguments for the event aggregator, parsed using clap. This includes NATS connection details,
// JetStream mode toggle, optional Loki endpoint for exporting, and subject filter for subscriptions.
// The `loki_url` is optional; if not provided, the aggregator will default to writing events to an ephemeral volume as JSON files.
#[derive(Parser)]
#[command(name = package_description!(), version = version_info_str!())]
struct CliArgs {
    /// NATS server URL.
    #[arg(long, short, default_value = "nats://mayastor-nats:4222")]
    nats_url: Url,

    /// Enable JetStream subscription mode.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    jetstream_enabled: bool,

    /// Endpoint of LOKI service, if left empty then it will try to parse endpoint
    /// from Loki service(K8s service resource), if the tool is unable to parse
    /// from service then logs will be collected using Kube-apiserver
    #[arg(global = true, short, long)]
    loki_endpoint: Option<Url>,

    /// The tenant id to be used to query loki logs.
    #[arg(global = true, long, default_value = "openebs")]
    tenant_id: String,

    // events subject filter
    #[arg(long, default_value = "events.>")]
    subject_filter: String,

    /// Local events directory for file exporter. Required when `--loki-url` is not provided.
    #[arg(long, short)]
    events_dir: Option<String>,

    /// Maximum total size for local event files under `--events-dir`, including rotated history.
    /// Supports human-readable units such as KiB, MiB, GiB, KB, MB, GB, bytes, and decimal values like 1.5GiB.
    /// Required when `--loki-url` is not provided.
    #[arg(long, short, value_parser = parse_dir_size_limit)]
    dir_size_limit: Option<usize>,
}
impl CliArgs {
    fn args() -> Self {
        CliArgs::parse()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize structured logging with tracing-subscriber
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli_args = CliArgs::args();

    // Setup Internal Channel
    let (tx, rx) = mpsc::channel::<UnifiedMessage>(CHANNEL_CAPACITY);

    info!("⚡🚀 [EVENT AGGREGATOR] Engine online and listening for cluster events...");
    tracing::info!(
        batch_max_size = exporter::BATCH_MAX_SIZE,
        batch_timeout_ms = exporter::BATCH_TIMEOUT_MS,
        "Exporter batch configuration"
    );

    // Initialize NATS Manager
    let nats_mgr = NatsManager::new(cli_args.nats_url.as_str())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to NATS: {}", e))?;
    nats_mgr
        .start_subscribing(cli_args.subject_filter, cli_args.jetstream_enabled, tx)
        .await?;

    let (tx_exporter, rx_exporter) = mpsc::channel::<LogEvent>(CHANNEL_CAPACITY);

    // Select exporter mode and spawn background exporter task.
    let exporter_mode = if let Some(url) = cli_args.loki_endpoint.clone() {
        exporter::ExporterMode::Loki {
            url: url.to_string(),
            client: reqwest::Client::new(),
        }
    } else {
        let events_dir = cli_args.events_dir.clone().ok_or_else(|| {
            anyhow::anyhow!("--events-dir is required when --loki-url is not provided")
        })?;

        let size_limit = cli_args.dir_size_limit.ok_or_else(|| {
            anyhow::anyhow!("--dir-size-limit is required when --loki-url is not provided")
        })?;

        tracing::info!(
            parsed_dir_size_bytes = size_limit,
            "Parsed directory size limit from CLI"
        );

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
// and sends structured log events to the exporter channel. It also manages ACK/NAK logic
// for JetStream messages based on processing results.
async fn unified_event_processor(
    mut rx: mpsc::Receiver<UnifiedMessage>,
    tx_exporter: mpsc::Sender<LogEvent>,
) {
    // Unified Processor Loop
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
                        // Transient failure: NAK with backoff for retry/redelivery
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
                        // Permanent failure (e.g., invalid JSON): ACK and discard to avoid wasted redeliveries
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
                // Core NATS is fire-and-forget: there is no ACK/NAK or retry path,
                // so we intentionally ignore the result here. Still attempt to send
                // the compact JSON to the exporter (non-blocking).
                let _ = process_message(&core_msg.subject, &core_msg.payload, Some(&tx_exporter));
            }
        }
    }
}

// Process a single message payload, attempting to parse it as JSON and log it in both colored multi-line format for console
// and compact single-line format for exporter. Returns a ProcessingResult indicating success or type of failure (transient vs permanent).
fn process_message(
    subject_str: &str,
    payload_bytes: &[u8],
    tx_exporter: Option<&mpsc::Sender<LogEvent>>,
) -> ProcessingResult {
    match serde_json::from_slice::<Value>(payload_bytes) {
        Ok(json_payload) => {
            let log_line = serde_json::json!({
                "fields": {
                    "subject": subject_str,
                    "payload": json_payload
                },
            });

            // Console: pretty / colored multi-line output for humans.
            match colored_json::to_colored_json_auto(&log_line) {
                Ok(colored_str) => {
                    info!("\n{colored_str}\n");
                }
                Err(err) => {
                    warn!(
                        subject = %subject_str,
                        error = %err,
                        "Failed to format event as colored JSON; falling back to plain JSON"
                    );
                    match serde_json::to_string_pretty(&log_line) {
                        Ok(plain_json) => {
                            info!("\n{plain_json}\n");
                        }
                        Err(serialize_err) => {
                            warn!(
                                subject = %subject_str,
                                error = %serialize_err,
                                "Failed to serialize event for plain JSON console output"
                            );
                        }
                    }
                }
            }

            // Prepare compact single-line JSON for crash-safe file storage / Loki ingestion.
            if let Ok(compact) = serde_json::to_string(&log_line) {
                if let Some(tx) = tx_exporter {
                    // Use non-blocking try_send to avoid awaiting in this sync handler.
                    if let Err(e) = tx.try_send(LogEvent {
                        subject: subject_str.to_string(),
                        line: compact,
                    }) {
                        warn!(subject = %subject_str, error = %e, "Exporter channel full or closed; dropping event for exporter");
                    }
                }
            }

            // Successfully parsed and processed the JSON payload
            ProcessingResult::Success
        }
        Err(e) => {
            const MAX_NON_JSON_PAYLOAD_LOG_BYTES: usize = 4096;
            let payload_size_bytes = payload_bytes.len();

            // Safely truncate at UTF-8 boundary to avoid splitting multi-byte characters
            let preview_len = payload_size_bytes.min(MAX_NON_JSON_PAYLOAD_LOG_BYTES);
            let safe_preview_len = if preview_len < payload_size_bytes {
                // If truncating, find the last valid UTF-8 boundary
                payload_bytes[..preview_len]
                    .iter()
                    .rposition(|&b| (b & 0xC0) != 0x80)
                    .map(|pos| pos + 1)
                    .unwrap_or(preview_len)
            } else {
                preview_len
            };

            let payload_preview = String::from_utf8_lossy(&payload_bytes[..safe_preview_len]);
            let payload_truncated = payload_size_bytes > MAX_NON_JSON_PAYLOAD_LOG_BYTES;

            warn!(
                subject = %subject_str,
                payload_preview = %payload_preview,
                payload_size_bytes = payload_size_bytes,
                payload_truncated = payload_truncated,
                error = %e,
                "Received non-JSON event payload; treating as permanent failure (will ACK, not retry)",
            );

            // Non-JSON payloads are permanent failures: ACK without retry to avoid wasted redelivery cycles
            ProcessingResult::PermanentFailure
        }
    }
}
