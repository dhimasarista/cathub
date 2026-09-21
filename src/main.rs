use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use dotenvy::dotenv;
use sqlx::any::{AnyPool, AnyPoolOptions};
use std::env;
use tracing::{info, warn};
// use rumqttd::{Broker, Config};

// Shared application state
struct AppState {
    db_pool: Option<AnyPool>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();
    info!("Starting CatHub - All-in-One IIoT Realtime Hub");

    // Load environment variables from .env if present
    dotenv().ok();

    // 1. Initialize Database (Generic DB Support with Fallback)
    // We install drivers for Postgres, MySQL, and SQLite.
    sqlx::any::install_default_drivers();
    
    let db_url = env::var("DATABASE_URL").unwrap_or_default();
    
    let db_pool = if db_url.is_empty() {
        warn!("DATABASE_URL not found in environment. Running in No-DB Mode (Memory/Broadcast only).");
        None
    } else {
        info!("DATABASE_URL detected. Attempting to connect...");
        match AnyPoolOptions::new().connect(&db_url).await {
            Ok(pool) => {
                info!("Database connected successfully.");
                Some(pool)
            }
            Err(e) => {
                warn!("Database connection failed: {}. Falling back to No-DB Mode.", e);
                None
            }
        }
    };

    let app_state = web::Data::new(AppState { db_pool });

    // 2. Start Embedded MQTT Broker (Running in a background task)
    tokio::spawn(async move {
        info!("Initializing Embedded MQTT Broker on port 1883...");
        // TODO: Configure rumqttd Broker here
        // let config = Config::default();
        // let mut broker = Broker::new(config);
        // broker.start().unwrap();
    });

    // 3. Start Broadcaster (Actix-Web HTTP/SSE Server)
    let addr = "0.0.0.0:3000";
    info!("Broadcaster listening on http://{}", addr);

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/health", web::get().to(health_check))
            .route("/api/stream", web::get().to(sse_handler))
    })
    .bind(addr)?
    .run()
    .await
}

// Health Check Endpoint
async fn health_check(data: web::Data<AppState>) -> impl Responder {
    let db_status = if data.db_pool.is_some() { "Connected" } else { "No-DB Mode" };
    HttpResponse::Ok().body(format!("CatHub is running deterministically. DB Status: {}", db_status))
}

// Placeholder for Server-Sent Events (SSE) Endpoint
async fn sse_handler() -> impl Responder {
    HttpResponse::Ok()
        .content_type("text/event-stream")
        .body("data: SSE Stream Endpoint (Under Construction)\n\n")
}
