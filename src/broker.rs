use crate::config::ModbusDevice;
use rumqttd::local::{LinkRx, LinkTx};
use rumqttd::{Broker, Config, ConnectionSettings, ConsoleSettings, RouterConfig, ServerSettings};
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::{error, info};

/// Builds a single-node rumqttd config with one plain MQTT v4 TCP listener
/// and its local diagnostics console. TLS, MQTT v5, websockets, clustering,
/// and Prometheus export are left unconfigured (`None`) since CatHub does
/// not need them yet.
///
/// `auth` maps username -> password for MQTT clients. Pass `None` to accept
/// unauthenticated connections, which is only appropriate on a trusted
/// network or during local development: the broker defaults to listening on
/// `0.0.0.0`, so an unauthenticated broker is reachable from anywhere that
/// can route to it.
#[allow(clippy::field_reassign_with_default)]
pub fn build_config(mqtt_listen: SocketAddr, auth: Option<HashMap<String, String>>) -> Config {
    let mut v4 = HashMap::new();
    v4.insert(
        "v4-1".to_string(),
        ServerSettings {
            name: "v4-1".to_string(),
            listen: mqtt_listen,
            tls: None,
            next_connection_delay_ms: 1,
            connections: ConnectionSettings {
                connection_timeout_ms: 60_000,
                max_payload_size: 20_480,
                max_inflight_count: 100,
                auth,
                dynamic_filters: true,
            },
        },
    );

    // `ConsoleSettings` has a private field, so it can't be built with
    // struct-literal (`..Default::default()`) syntax from outside the
    // crate; mutate its public `listen` field on a default instance instead.
    let mut console = ConsoleSettings::default();
    console.listen = "127.0.0.1:3030".to_string();

    // `Config` also has a field (`console`, transitively) that blocks
    // `..Default::default()` struct-literal syntax; same workaround as above.
    let mut config = Config::default();
    config.router = RouterConfig {
        max_connections: 10_010,
        max_outgoing_packet_count: 200,
        max_segment_size: 104_857_600,
        max_segment_count: 10,
        ..Default::default()
    };
    config.v4 = v4;
    config.console = console;
    config
}

/// Runs the broker's network listeners and console on a dedicated OS
/// thread. `Broker::start` blocks the calling thread forever, so it must
/// never be awaited directly inside the tokio runtime.
pub fn spawn(mut broker: Broker) {
    std::thread::Builder::new()
        .name("mqtt-broker".to_string())
        .spawn(move || {
            info!("Embedded MQTT broker starting");
            if let Err(e) = broker.start() {
                error!(error = ?e, "MQTT broker exited with an error");
            }
        })
        .expect("failed to spawn MQTT broker thread");
}

/// The internal, in-process MQTT links the rest of CatHub needs. All of
/// them must be created before the broker is moved into its own thread
/// (`spawn` above), since `Broker::link` needs `&self` and `Broker::start`
/// needs `&mut self`.
pub struct MqttHandles {
    pub ingest_rx: LinkRx,
    pub alert_tx: LinkTx,
    pub modbus_links: Vec<(ModbusDevice, LinkTx)>,
}

/// Builds the broker, wires up CatHub's internal links, and starts the
/// broker's network listeners on their own thread.
pub fn bootstrap(
    mqtt_listen: SocketAddr,
    auth: Option<HashMap<String, String>>,
    modbus_devices: &[ModbusDevice],
) -> MqttHandles {
    let rumqttd_broker = Broker::new(build_config(mqtt_listen, auth));

    let (mut ingest_tx, ingest_rx) = rumqttd_broker
        .link("cathub-ingest")
        .expect("failed to create the internal ingestion link to the MQTT broker");
    ingest_tx
        .subscribe("#")
        .expect("failed to subscribe the ingestion link to all topics");

    let (alert_tx, _unused_alert_rx) = rumqttd_broker
        .link("cathub-alerts")
        .expect("failed to create the internal alert-publishing link to the MQTT broker");

    let modbus_links = modbus_devices
        .iter()
        .map(|device| {
            let (tx, _unused_rx) = rumqttd_broker
                .link(&format!("cathub-modbus-{}", device.name))
                .expect("failed to create an internal Modbus bridge link to the MQTT broker");
            (device.clone(), tx)
        })
        .collect();

    spawn(rumqttd_broker);

    MqttHandles {
        ingest_rx,
        alert_tx,
        modbus_links,
    }
}
