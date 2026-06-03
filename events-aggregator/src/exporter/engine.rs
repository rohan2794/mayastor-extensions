use byte_unit::Byte;
use std::{
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::time::delay_queue::{DelayQueue, Key};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::MakeWriter;

// Configuration constants for batch processing and retry logic.
pub const BATCH_MAX_SIZE: usize = 100;
pub const BATCH_TIMEOUT_MS: u64 = 10000; // 10 seconds

/// Parse a CLI directory size limit string into a `usize` byte count.
/// Supports human-readable values such as `10MiB`, `1.5GiB`, `10000`, and `8 KB`.
pub fn parse_dir_size_limit(value: &str) -> Result<usize, String> {
    let parsed =
        Byte::parse_str(value, true).map_err(|e| format!("invalid size '{value}': {e}"))?;
    parsed
        .as_u64_checked()
        .ok_or_else(|| format!("size limit '{value}' exceeds usize maximum"))
        .and_then(|bytes| {
            usize::try_from(bytes)
                .map_err(|_| format!("size limit '{value}' exceeds usize maximum"))
        })
}
const MAX_RETRIES: usize = 3;
const APPLICATION_NAME: &str = "events-aggregator";

/// A single event produced by the aggregator pipeline.
/// `subject` is used as the Loki stream label, while `line` is the payload.
#[derive(Debug)]
pub struct LogEvent {
    pub subject: String,
    pub line: String,
}

/// ExporterMode defines the target destination for log events, either Loki or a local file.
pub enum ExporterMode {
    Loki {
        url: String,
        client: reqwest::Client,
    },
    File {
        dir: PathBuf,
        size_limit: usize,
    },
}

/// Main exporter loop that receives log events, batches them, and flushes to the
/// configured destination based on size or timeout triggers.
pub async fn run_exporter(mode: ExporterMode, mut rx: mpsc::Receiver<LogEvent>) {
    let mut batch: Vec<LogEvent> = Vec::with_capacity(BATCH_MAX_SIZE);
    let mut delay_queue = DelayQueue::new();
    let mut timeout_key: Option<Key> = None;

    // Choose destination mode and log the configured target.
    match &mode {
        ExporterMode::Loki { url, .. } => {
            tracing::info!(target_url = %url, "Batch Exporter in Loki-only mode.")
        }
        ExporterMode::File { dir, .. } => {
            tracing::info!(target_path = ?dir, "Batch Exporter in File-only mode.")
        }
    }

    loop {
        tokio::select! {
            // Case A: Process a new incoming telemetry event from the channel pipeline
            Some(event) = rx.recv() => {
                if batch.is_empty() {
                    let delay = Duration::from_millis(BATCH_TIMEOUT_MS);
                    timeout_key = Some(delay_queue.insert((), delay));
                }

                batch.push(event);

                if batch.len() >= BATCH_MAX_SIZE {
                    if let Some(key) = timeout_key.take() {
                        delay_queue.remove(&key);
                    }
                    tracing::info!(
                        action = "flush_trigger",
                        reason = "max_batch_size",
                        batch_len = batch.len(),
                        batch_max = BATCH_MAX_SIZE,
                        "Triggering flush due to max batch size"
                    );
                    flush_batch_with_retry(&mode, &mut batch).await;
                }
            }

            // Case B: The flush timeout window expired
            Some(_) = delay_queue.next(), if timeout_key.is_some() => {
                timeout_key = None;
                if !batch.is_empty() {
                    tracing::info!(
                        action = "flush_trigger",
                        reason = "timeout",
                        batch_len = batch.len(),
                        timeout_ms = BATCH_TIMEOUT_MS,
                        "Triggering flush due to timeout"
                    );
                    flush_batch_with_retry(&mode, &mut batch).await;
                }
            }

            // Fallback termination boundary if main channel engine disconnects
            else => break,
        }
    }

    if !batch.is_empty() {
        // Final shutdown flush for any remaining buffered events.
        tracing::info!(
            action = "flush_trigger",
            reason = "shutdown",
            batch_len = batch.len(),
            "Triggering final flush on shutdown"
        );
        flush_batch_with_retry(&mode, &mut batch).await;
    }
}

/// Iterates target egress flushes using structured exponential backoff delays
async fn flush_batch_with_retry(mode: &ExporterMode, batch: &mut Vec<LogEvent>) {
    let mut attempts = 0;
    let mut backoff = Duration::from_millis(500);
    let mut success = false;

    let dest = match mode {
        ExporterMode::Loki { .. } => "loki",
        ExporterMode::File { .. } => "file",
    };

    tracing::info!(
        action = "flush_start",
        destination = dest,
        batch_size = batch.len(),
        "Starting flush of batched events"
    );

    // Retry on transient failures using exponential backoff.
    while attempts < MAX_RETRIES {
        let result = match mode {
            ExporterMode::Loki { url, client } => ship_to_loki(client, url, batch).await,
            ExporterMode::File { dir, size_limit } => write_to_disk(dir, *size_limit, batch).await,
        };

        match result {
            Ok(_) => {
                success = true;
                tracing::info!(
                    action = "flush_success",
                    destination = dest,
                    attempts = attempts,
                    "Flush completed successfully"
                );
                break;
            }
            Err(e) => {
                attempts += 1;
                tracing::warn!(
                    attempt = attempts,
                    error = %e,
                    "Egress write failed. Backing off before retry..."
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
    }

    if !success {
        tracing::error!(
            "Failed to flush batch of {} messages after {} retries. Dropping batch.",
            batch.len(),
            MAX_RETRIES
        );
    }

    let cleared = batch.len();
    batch.clear();
    tracing::info!(
        action = "buffer_cleared",
        cleared = cleared,
        "Cleared in-memory batch buffer after flush attempt"
    );
}

// Ship a batch of log events to Loki using the push API. Each event is sent as a
// separate stream entry with the subject as a label.
async fn ship_to_loki(
    client: &reqwest::Client,
    url: &str,
    batch: &[LogEvent],
) -> anyhow::Result<()> {
    let push_endpoint = format!("{}/loki/api/v1/push", url.trim_end_matches('/'));

    // Build one Loki stream entry per log event.
    let mut streams = serde_json::json!([]);
    for event in batch {
        // Use a fresh timestamp per event to preserve ordering and timing.
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_string();
        streams.as_array_mut().unwrap().push(serde_json::json!({
            "stream": { "app": APPLICATION_NAME, "subject": event.subject },
            "values": [ [ timestamp.as_str(), event.line ] ]
        }));
    }

    let body = serde_json::json!({ "streams": streams });

    // Build the request with the required multi-tenant header
    let response = client
        .post(&push_endpoint)
        .header("X-Scope-OrgID", "openebs")
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let err_body = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown API error payload".to_string());
        return Err(anyhow::anyhow!(
            "Loki rejected packet [Status {}]: {}",
            status,
            err_body
        ));
    }
    Ok(())
}

// Append a batch of log events to a local file, ensuring the target directory exists and handling file I/O asynchronously.
async fn write_to_disk(dir: &Path, size_limit: usize, batch: &[LogEvent]) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(dir).await?;

    let mut buffer = String::new();
    for event in batch {
        buffer.push_str(&event.line);
        buffer.push('\n');
    }

    // The configured size limit allows up to 40% of the limit in `events.json` and
    // another 40% in `events.1.json`. When `events.json` would exceed 40%, we rotate it.
    let pending = buffer.len();
    tracing::info!(action = "prepare_disk_write", dir = ?dir, pending_bytes = pending, size_limit = size_limit, "Preparing to write batch to disk");
    rotate_file_if_needed(dir, size_limit, pending).await?;

    let dir = dir.to_path_buf();
    let buffer = buffer.clone();

    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let appender = RollingFileAppender::new(Rotation::NEVER, dir, "events.json");
        let mut writer = appender.make_writer();
        writer.write_all(buffer.as_bytes())?;
        writer.flush()?;
        Ok(())
    })
    .await??;

    Ok(())
}

async fn rotate_file_if_needed(
    dir: &Path,
    size_limit: usize,
    pending_bytes: usize,
) -> anyhow::Result<()> {
    let base_path = dir.join("events.json");
    let rotated_path = dir.join("events.1.json");

    let current_size = tokio::fs::metadata(&base_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    let mut rotated_size = tokio::fs::metadata(&rotated_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    let pending_size = pending_bytes as u64;
    let max_total_size = size_limit as u64;

    let per_file_limit = max_total_size * 40 / 100;

    tracing::info!(
        action = "rotate_check",
        current_size = current_size,
        rotated_size = rotated_size,
        pending_size = pending_size,
        per_file_limit = per_file_limit,
        "Checking whether rotation is required for log files"
    );

    if current_size + pending_size > per_file_limit {
        tracing::info!(
            action = "rotate_needed",
            current_size = current_size,
            pending_size = pending_size,
            per_file_limit = per_file_limit,
            "Current file plus pending exceeds per-file limit; performing rotation steps"
        );

        if rotated_size > 0 {
            tracing::info!(action = "remove_existing_rotated", rotated_path = ?rotated_path, rotated_size = rotated_size, "Removing existing rotated file before rotating");
            tokio::fs::remove_file(&rotated_path).await.ok();
            rotated_size = 0;
        }

        if current_size > 0 {
            tracing::info!(action = "rename_base_to_rotated", from = ?base_path, to = ?rotated_path, size = current_size, "Rotating current base file into rotated slot");
            tokio::fs::rename(&base_path, &rotated_path).await?;
            rotated_size = current_size;
        }
    } else {
        tracing::debug!(
            action = "rotate_not_needed",
            current_size = current_size,
            rotated_size = rotated_size,
            pending_size = pending_size,
            "Rotation not required at this time"
        );
    }

    if rotated_size > per_file_limit {
        tracing::info!(action = "remove_rotated_oversize", rotated_path = ?rotated_path, rotated_size = rotated_size, per_file_limit = per_file_limit, "Removing rotated file because it exceeds per-file limit");
        tokio::fs::remove_file(&rotated_path).await.ok();
    }

    Ok(())
}
