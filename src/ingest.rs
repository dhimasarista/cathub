use crate::alerts::{self, AlertEngine};
use crate::cache::{StateCache, UpdateOutcome};
use crate::vault::{self, Backend};
use chrono::Utc;
use rumqttd::local::{LinkRx, LinkTx};
use rumqttd::Notification;
use serde_json::json;
use sqlx::AnyPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, warn};

/// Matches the vault's `telemetry_latest.topic VARCHAR(255)` column on
/// MySQL (Postgres/SQLite use unbounded TEXT, but one shared limit is
/// simplest). A topic longer than this is rejected rather than truncated,
/// so it never silently merges with a different topic sharing a prefix.
const MAX_TOPIC_LEN: usize = 255;

pub struct IngestContext {
    pub cache: Arc<StateCache>,
    pub bus: broadcast::Sender<String>,
    pub db: Option<(AnyPool, Backend)>,
    pub alerts: Arc<AlertEngine>,
}

/// The core pipeline: every MQTT message the broker forwards to this link
/// passes through dedup (via the state cache), then fans out to the SSE
/// broadcaster, the vault, and the alert engine. A payload identical to the
/// last one seen for its topic short-circuits here and never reaches any of
/// those three, which is what makes persistence and alerting idempotent.
pub async fn run(mut rx: LinkRx, mut alert_tx: LinkTx, ctx: IngestContext) {
    loop {
        let notification = match rx.next().await {
            Ok(Some(n)) => n,
            Ok(None) => continue,
            Err(e) => {
                error!(error = ?e, "MQTT link closed; stopping ingestion");
                break;
            }
        };

        let Notification::Forward(forward) = notification else {
            continue;
        };

        let topic = String::from_utf8_lossy(&forward.publish.topic).into_owned();
        if topic.len() > MAX_TOPIC_LEN {
            warn!(topic_len = topic.len(), limit = MAX_TOPIC_LEN, "Dropping message: topic exceeds max length");
            continue;
        }
        let payload_str = String::from_utf8_lossy(&forward.publish.payload).into_owned();

        let value: serde_json::Value = serde_json::from_str(&payload_str)
            .unwrap_or_else(|_| serde_json::Value::String(payload_str.clone()));

        if matches!(
            ctx.cache.update(&topic, &payload_str, value.clone()),
            UpdateOutcome::Unchanged
        ) {
            continue;
        }

        let event = json!({
            "topic": topic,
            "payload": value,
            "ts": Utc::now().to_rfc3339(),
        })
        .to_string();
        // No subscribers is not an error: it just means no dashboard is
        // currently connected to /api/stream.
        let _ = ctx.bus.send(event);

        if let Some((pool, backend)) = &ctx.db {
            if let Err(e) = vault::upsert_latest(pool, *backend, &topic, &payload_str).await {
                warn!(error = %e, topic = %topic, "Failed to persist telemetry to the vault");
            }
        }

        for triggered in ctx.alerts.evaluate(&topic, &value) {
            alerts::dispatch(&triggered, &mut alert_tx);
        }
    }
}
