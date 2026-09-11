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

    /// Keep prompts and request settings in the transcript while omitting the
    /// schema and transport flag. This display copy never changes the HTTP body.
    pub(crate) fn request(&mut self, endpoint: &str, request: &impl serde::Serialize) {
        match serde_json::to_value(request) {
            Ok(mut value) => {
                if let Some(fields) = value.as_object_mut() {
                    fields.remove("response_format");
                    fields.remove("stream");
                }
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

    /// Show only answer, reasoning, and provider token counts. Absent/malformed
    /// fields stay unavailable; the grouped RESULT owns the failure explanation.
    pub(crate) fn response(&mut self, bytes: Option<&[u8]>) {
        let response = bytes.and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok());
        let message = response
            .as_ref()
            .and_then(|value| value.pointer("/choices/0/message"));
        let content = message
            .and_then(|message| message.get("content"))
            .and_then(Value::as_str);
        // Providers use either spelling; this is the same protocol distinction
        // checked by the client, which still rejects supplying both at once.
        let reasoning = message.and_then(|message| {
            message
                .get("reasoning")
                .and_then(Value::as_str)
                .or_else(|| message.get("reasoning_content").and_then(Value::as_str))
        });
        let mut body = format!(
            "Content:\n{}\n\nReasoning:\n{}\n\n",
            content.unwrap_or("unavailable"),
            reasoning.unwrap_or("unavailable"),
        );
        for (label, path) in [
            ("Completion tokens", "/usage/completion_tokens"),
            (
                "Reasoning tokens",
                "/usage/completion_tokens_details/reasoning_tokens",
            ),
            ("Prompt tokens", "/usage/prompt_tokens"),
            ("Total tokens", "/usage/total_tokens"),
        ] {
            let count = response
                .as_ref()
                .and_then(|value| value.pointer(path))
                .and_then(Value::as_u64);
            // Formatting into String cannot encounter an I/O failure.
            match count {
                Some(count) => {
                    let _ = writeln!(body, "{label}: {count}");
                }
                None => {
                    let _ = writeln!(body, "{label}: unavailable");
                }
            }
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
