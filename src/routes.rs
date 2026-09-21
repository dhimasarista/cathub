use crate::alerts::AlertEngine;
use crate::cache::StateCache;
use crate::vault::Backend;
use actix_web::{web, HttpResponse, Responder};
use futures_util::StreamExt;
use serde_json::json;
use sqlx::AnyPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

pub struct AppState {
    pub db_pool: Option<AnyPool>,
    #[allow(dead_code)] // reserved for future backend-specific admin endpoints
    pub db_backend: Option<Backend>,
    pub cache: Arc<StateCache>,
    pub bus: broadcast::Sender<String>,
    pub alerts: Arc<AlertEngine>,
    pub modbus_device_count: usize,
}

pub async fn health_check(data: web::Data<AppState>) -> impl Responder {
    let db_status = if data.db_pool.is_some() {
        "Connected"
    } else {
        "No-DB Mode"
    };

    HttpResponse::Ok().json(json!({
        "status": "ok",
        "database": db_status,
        "modbus_devices": data.modbus_device_count,
        "alert_rules": data.alerts.len(),
    }))
}

/// Streams every deduplicated, ingested message as Server-Sent Events.
/// A slow subscriber that falls behind the broadcast channel's buffer sees
/// its missed events silently dropped rather than the connection killed.
pub async fn sse_handler(data: web::Data<AppState>) -> impl Responder {
    let receiver = data.bus.subscribe();
    let stream = BroadcastStream::new(receiver).filter_map(|item| async move {
        match item {
            Ok(event) => Some(Ok::<_, actix_web::Error>(web::Bytes::from(format!(
                "data: {event}\n\n"
            )))),
            Err(_lagged) => None,
        }
    });

    HttpResponse::Ok()
        .content_type("text/event-stream")
        .append_header(("Cache-Control", "no-cache"))
        .streaming(stream)
}

pub async fn state_snapshot(data: web::Data<AppState>) -> impl Responder {
    HttpResponse::Ok().json(data.cache.snapshot())
}

pub async fn state_for_topic(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    match data.cache.get(&path.into_inner()) {
        Some(value) => HttpResponse::Ok().json(value),
        None => HttpResponse::NotFound().json(json!({"error": "topic not found"})),
    }
}
