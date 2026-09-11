//! Human-readable annotator exchanges, separate from compact service diagnostics.

use std::{
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Instant,
};

use serde_json::Value;
use tracing::{error, info};
use tracing_subscriber::fmt::{format::Writer, time::FormatTime};

use crate::util::{LogContext, diagnostic_id};

/// One append-only sink shared by all clones of an annotator client. Holding the
/// lock across a complete call group prevents concurrent exchanges interleaving.
#[derive(Debug)]
pub(crate) struct Transcript {
    path: PathBuf,
    file: Mutex<Option<File>>,
}

/// One producer owns the buffered exchange until the terminal result consumes it.
/// Logging context is owned separately so no lock is held while the model runs.
pub(crate) struct TranscriptCall<'sink> {
    sink: &'sink Transcript,
    id: String,
    pub(crate) stage: &'static str,
    started_at: Instant,
    request: Option<String>,
    response: Option<String>,
}

impl Transcript {
    /// Open beside the configuration's logs directory in both service and dry-run
    /// modes. Diagnostic failures are visible but do not change annotation outcomes.
    pub(crate) fn open(config_root: &Path) -> Self {
        let path = config_root.join("logs/annotator.log");
        let opened = fs::create_dir_all(config_root.join("logs"))
            .and_then(|()| OpenOptions::new().create(true).append(true).open(&path));
        let file = match opened {
            Ok(file) => {
                info!(event = "annotator_transcript.opened", path = %path.display(),
                    "annotator transcript opened for append");
                Some(file)
            }
            Err(source) => {
                error!(event = "annotator_transcript.open_failed", path = %path.display(),
                    error = %source, "annotator transcript unavailable");
                None
            }
        };
        Self {
            path,
            file: Mutex::new(file),
        }
    }

    /// Return independent owners for mutable transcript data and immutable tracing
    /// context, both carrying the same identity through HTTP and validation.
    pub(crate) fn call(&self, stage: &'static str) -> (TranscriptCall<'_>, LogContext) {
        let id = diagnostic_id("call");
        let context = LogContext::new("model_call", &id);
        context.record("model_role", "annotator");
        context.record("call_purpose", stage);
        context.record("stage", stage);
        let call = TranscriptCall {
            sink: self,
            id,
            stage,
            started_at: Instant::now(),
            request: None,
            response: None,
        };
        (call, context)
    }

    /// Write and flush the whole group under one lock; report every failure through
    /// the service log without recursively writing to the failed transcript sink.
    fn append(&self, block: &str) -> io::Result<()> {
        let mut guard = self
            .file
            .lock()
            .map_err(|_| io::Error::other("annotator transcript lock is poisoned"))?;
        let file = guard
            .as_mut()
            .ok_or_else(|| io::Error::other("annotator transcript did not open"))?;
        file.write_all(block.as_bytes())?;
        file.flush()
    }
}

impl TranscriptCall<'_> {
    /// Append exactly once after validation, failure, or cancellation. Buffering
    /// keeps related sections contiguous without serializing model execution;
    /// abrupt process termination can lose a call whose result is not yet known.
    pub(crate) fn result(self, outcome: &str, reason: &str) {
        let mut timestamp = String::new();
        if let Err(source) =
            tracing_subscriber::fmt::time::SystemTime.format_time(&mut Writer::new(&mut timestamp))
        {
            error!(event = "annotator_transcript.timestamp_failed", error = %source,
                call_id = %self.id, "failed to format annotator transcript time");
        }
        let block = format!(
            "\n===== ANNOTATOR CALL — {} =====\nTime: {timestamp}\nStage: {}\nElapsed: {} ms\n\nREQUEST\n{}\n\nRESPONSE\n{}\n\nRESULT\n{outcome}\n{reason}\n===== END CALL — {} =====\n",
            self.id,
            self.stage,
            self.started_at.elapsed().as_millis(),
            self.request.as_deref().unwrap_or("No request prepared."),
            self.response.as_deref().unwrap_or("No response received."),
            self.id,
        );
        if let Err(source) = self.sink.append(&block) {
            error!(event = "annotator_transcript.write_failed", path = %self.sink.path.display(),
                call_id = %self.id, stage = self.stage, block_kind = "CALL", error = %source,
                "annotator transcript group could not be written");
        }
    }

    /// Render all fields from the actual request object, including strings that
    /// carry prompts or stage input, and retain them until the call has a result.
    pub(crate) fn request(&mut self, endpoint: &str, request: &impl serde::Serialize) {
        match serde_json::to_value(request) {
            Ok(value) => {
                let mut body = format!("POST {endpoint}\n\n");
                render_value(&value, 0, &mut body);
                self.request = Some(body);
            }
            Err(source) => {
                self.request = Some(format!("Request could not be rendered: {source}"));
                error!(event = "annotator_transcript.serialization_failed",
                    call_id = %self.id, error = %source, "could not render annotator request");
            }
        }
    }

    /// Render the complete provider response once, including reasoning, usage,
    /// unknown fields, and errors; retain it for the same group as validation.
    pub(crate) fn response(&mut self, status: Option<u16>, bytes: Option<&[u8]>, outcome: &str) {
        let mut body = format!(
            "HTTP status: {}\nHTTP/protocol result: {outcome}\n\n",
            status.map_or_else(|| "not received".to_string(), |status| status.to_string())
        );
        match bytes {
            Some(bytes) => match serde_json::from_slice::<Value>(bytes) {
                Ok(value) => render_value(&value, 0, &mut body),
                Err(_) => match std::str::from_utf8(bytes) {
                    Ok(text) => body.push_str(text),
                    Err(_) => {
                        body.push_str("INVALID UTF-8 RESPONSE (hex)\n");
                        for byte in bytes {
                            // Formatting into String cannot encounter an I/O failure.
                            let _ = write!(body, "{byte:02x} ");
                        }
                    }
                },
            },
            None => body.push_str("No complete response body received.\n"),
        }
        self.response = Some(body);
    }
}

/// Preserve all JSON fields while displaying string contents literally. Array
/// indexes and indentation retain structure without escaped JSON inside JSON.
fn render_value(value: &Value, indent: usize, output: &mut String) {
    let padding = " ".repeat(indent);
    match value {
        Value::Object(fields) if !fields.is_empty() => {
            for (key, value) in fields {
                let _ = writeln!(output, "{padding}{key}:");
                render_value(value, indent + 2, output);
            }
        }
        Value::Array(values) if !values.is_empty() => {
            for (index, value) in values.iter().enumerate() {
                let _ = writeln!(output, "{padding}[{index}]:");
                render_value(value, indent + 2, output);
            }
        }
        Value::String(text) => {
            for line in text.split('\n') {
                let _ = writeln!(output, "{padding}{line}");
            }
        }
        value => {
            let _ = writeln!(output, "{padding}{value}");
        }
    }
}
