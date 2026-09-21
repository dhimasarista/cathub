mod alerts;
mod broker;
mod cache;
mod config;
mod ingest;
mod modbus_bridge;
mod routes;
mod vault;

use actix_web::{web, App, HttpServer};
use cache::StateCache;
use config::AppConfig;
use dotenvy::dotenv;
use routes::{health_check, sse_handler, state_for_topic, state_snapshot, AppState};
use sqlx::any::AnyPoolOptions;
use sqlx::AnyPool;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

const EVENT_BUS_CAPACITY: usize = 256;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting CatHub - All-in-One IIoT Realtime Hub");

    dotenv().ok();

    let app_config = AppConfig::load(Path::new("cathub.toml")).unwrap_or_else(|e| {
        warn!(
            error = %e,
            "Failed to load cathub.toml; continuing with no Modbus devices or alert rules"
        );
        AppConfig::default()
    });

    let db = connect_vault().await;
    let mqtt = bootstrap_mqtt(&app_config);

    // Shared state.
    let cache = Arc::new(StateCache::new());
    let (bus, _unused_bus_rx) = broadcast::channel::<String>(EVENT_BUS_CAPACITY);
    let alert_engine = Arc::new(alerts::AlertEngine::new(app_config.alerts.clone()));

    info!(
        modbus_devices = app_config.modbus.len(),
        alert_rules = alert_engine.len(),
        "Configuration loaded"
    );

    for (device, tx) in mqtt.modbus_links {
        tokio::spawn(modbus_bridge::run(device, tx));
    }

    tokio::spawn(ingest::run(
        mqtt.ingest_rx,
        mqtt.alert_tx,
        ingest::IngestContext {
            cache: cache.clone(),
            bus: bus.clone(),
            db: db.clone(),
            alerts: alert_engine.clone(),
        },
    ));

    let app_state = web::Data::new(AppState {
        db_pool: db.as_ref().map(|(pool, _)| pool.clone()),
        db_backend: db.as_ref().map(|(_, backend)| *backend),
        cache,
        bus,
        alerts: alert_engine,
        modbus_device_count: app_config.modbus.len(),
    });

    let addr = env::var("HTTP_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());
    info!("Broadcaster listening on http://{}", addr);

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/health", web::get().to(health_check))
            .route("/api/stream", web::get().to(sse_handler))
            .route("/api/state", web::get().to(state_snapshot))
            .route("/api/state/{topic:.*}", web::get().to(state_for_topic))
    })
    .bind(&addr)?
    .run()
    .await
}

/// Parses `MQTT_LISTEN_ADDR`/`MQTT_USERNAME`/`MQTT_PASSWORD`, builds the
/// embedded broker, and wires up CatHub's internal MQTT links. Split out of
/// `main` purely to keep `main` itself short; see `broker::bootstrap` for
/// the actual link-ordering constraints this has to respect.
fn bootstrap_mqtt(app_config: &AppConfig) -> broker::MqttHandles {
    let mqtt_listen: SocketAddr = env::var("MQTT_LISTEN_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:1883".to_string())
        .parse()
        .expect("MQTT_LISTEN_ADDR must be a valid socket address");

    let mqtt_auth = match (env::var("MQTT_USERNAME"), env::var("MQTT_PASSWORD")) {
        (Ok(user), Ok(pass)) if !user.is_empty() => Some(HashMap::from([(user, pass)])),
        _ => {
            warn!(
                "MQTT_USERNAME/MQTT_PASSWORD not set: the embedded broker will accept \
                 unauthenticated connections. Fine for local development on a trusted \
                 network; set both before exposing MQTT_LISTEN_ADDR beyond localhost."
            );
            None
        }
    };

    broker::bootstrap(mqtt_listen, mqtt_auth, &app_config.modbus)
}

/// Connects to the vault and ensures its schema exists. Any failure along
/// the way (missing URL, unrecognized scheme, connection error, schema
/// error) is logged and treated as "run in No-DB mode" rather than a fatal
/// error, matching CatHub's original broadcast-only fallback behavior.
async fn connect_vault() -> Option<(AnyPool, vault::Backend)> {
    sqlx::any::install_default_drivers();
    let db_url = env::var("DATABASE_URL").unwrap_or_default();

    if db_url.is_empty() {
        warn!("DATABASE_URL not found in environment. Running in No-DB Mode (memory/broadcast only).");
        return None;
    }

    let Some(backend) = vault::Backend::detect(&db_url) else {
        warn!("DATABASE_URL scheme not recognized. Running in No-DB Mode.");
        return None;
    };

    let pool = match AnyPoolOptions::new().connect(&db_url).await {
        Ok(pool) => pool,
        Err(e) => {
            warn!(error = %e, "Database connection failed. Falling back to No-DB Mode.");
            return None;
        }
    };

    if let Err(e) = vault::ensure_schema(&pool, backend).await {
        warn!(error = %e, "Failed to prepare vault schema. Falling back to No-DB Mode.");
        return None;
    }

    info!(?backend, "Vault connected and schema ensured");
    Some((pool, backend))
}
