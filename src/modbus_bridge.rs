use crate::config::ModbusDevice;
use chrono::Utc;
use rumqttd::local::LinkTx;
use serde_json::json;
use std::net::SocketAddr;
use std::time::Duration;
use tokio_modbus::client::{tcp, Client, Reader};
use tokio_modbus::Slave;
use tracing::{info, warn};

/// Polls one Modbus TCP device on a fixed interval and republishes each
/// configured register's reading as an MQTT message, so downstream ingestion
/// (dedup, cache, vault, alerts, SSE) never has to know a device isn't
/// speaking MQTT natively.
///
/// The connection is re-established every poll cycle rather than kept alive
/// and reconnected on error. This is simpler and more robust against
/// half-open sockets at the cost of reconnect overhead on each tick; for
/// high-frequency polling this would be worth revisiting.
pub async fn run(device: ModbusDevice, mut tx: LinkTx) {
    let addr: SocketAddr = match device.address.parse() {
        Ok(a) => a,
        Err(e) => {
            warn!(
                device = %device.name,
                error = %e,
                "Invalid Modbus address; this device will not be polled"
            );
            return;
        }
    };

    if device.poll_interval_ms == 0 {
        warn!(
            device = %device.name,
            "poll_interval_ms must be non-zero; this device will not be polled"
        );
        return;
    }

    info!(device = %device.name, %addr, "Starting Modbus polling loop");
    let mut ticker = tokio::time::interval(Duration::from_millis(device.poll_interval_ms));

    loop {
        ticker.tick().await;

        let mut ctx = match tcp::connect_slave(addr, Slave(device.slave_id)).await {
            Ok(c) => c,
            Err(e) => {
                warn!(device = %device.name, error = %e, "Modbus TCP connect failed");
                continue;
            }
        };

        for register in &device.registers {
            match ctx
                .read_holding_registers(register.address, register.quantity)
                .await
            {
                Ok(Ok(values)) => {
                    let payload = json!({
                        "device": device.name,
                        "register": register.name,
                        "value": values.first().copied(),
                        "values": values,
                        "ts": Utc::now().to_rfc3339(),
                    })
                    .to_string();

                    if let Err(e) = tx.publish(register.topic.clone(), payload) {
                        warn!(
                            device = %device.name,
                            register = %register.name,
                            error = ?e,
                            "Failed to publish Modbus reading to the broker"
                        );
                    }
                }
                Ok(Err(exception)) => {
                    warn!(
                        device = %device.name,
                        register = %register.name,
                        ?exception,
                        "Modbus device returned an exception response"
                    );
                }
                Err(e) => {
                    warn!(
                        device = %device.name,
                        register = %register.name,
                        error = %e,
                        "Modbus transport error"
                    );
                }
            }
        }

        let _ = ctx.disconnect().await;
    }
}
