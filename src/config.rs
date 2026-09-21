use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub modbus: Vec<ModbusDevice>,
    #[serde(default)]
    pub alerts: Vec<AlertRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModbusDevice {
    pub name: String,
    /// Modbus TCP gateway address, e.g. "192.168.1.50:502".
    pub address: String,
    #[serde(default = "default_slave_id")]
    pub slave_id: u8,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    pub registers: Vec<RegisterMap>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterMap {
    pub name: String,
    /// Holding register start address (function code 0x03).
    pub address: u16,
    #[serde(default = "default_quantity")]
    pub quantity: u16,
    /// MQTT topic this register's reading is published to.
    pub topic: String,
}

fn default_slave_id() -> u8 {
    1
}

fn default_poll_interval_ms() -> u64 {
    5_000
}

fn default_quantity() -> u16 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct AlertRule {
    pub name: String,
    /// Exact MQTT topic to watch. Wildcards ("+", "#") are not supported yet.
    pub topic: String,
    /// Top-level numeric field in the topic's JSON payload to evaluate.
    pub field: String,
    pub operator: Operator,
    pub threshold: f64,
    /// Re-publish a JSON alert to this MQTT topic when the rule fires.
    #[serde(default)]
    pub publish_topic: Option<String>,
    /// POST a JSON alert to this URL when the rule fires.
    #[serde(default)]
    pub webhook_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
    Equal,
}

impl Operator {
    pub fn evaluate(self, value: f64, threshold: f64) -> bool {
        match self {
            Operator::GreaterThan => value > threshold,
            Operator::GreaterOrEqual => value >= threshold,
            Operator::LessThan => value < threshold,
            Operator::LessOrEqual => value <= threshold,
            Operator::Equal => (value - threshold).abs() < f64::EPSILON,
        }
    }
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(toml::de::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "failed to read config file: {e}"),
            ConfigError::Parse(e) => write!(f, "failed to parse config file: {e}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<std::io::Error> for ConfigError {
    fn from(e: std::io::Error) -> Self {
        ConfigError::Io(e)
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(e: toml::de::Error) -> Self {
        ConfigError::Parse(e)
    }
}

impl AppConfig {
    /// Loads config from `path`. A missing file is not an error: it means
    /// "no Modbus devices, no alert rules" (both features are opt-in).
    pub fn load(path: &Path) -> Result<AppConfig, ConfigError> {
        if !path.exists() {
            return Ok(AppConfig::default());
        }
        let raw = fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&raw)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_yields_default_config() {
        let config = AppConfig::load(Path::new("does-not-exist.toml")).unwrap();
        assert!(config.modbus.is_empty());
        assert!(config.alerts.is_empty());
    }

    #[test]
    fn operator_evaluate_matches_expected_semantics() {
        assert!(Operator::GreaterThan.evaluate(10.0, 5.0));
        assert!(!Operator::GreaterThan.evaluate(5.0, 10.0));
        assert!(Operator::LessOrEqual.evaluate(5.0, 5.0));
        assert!(Operator::Equal.evaluate(5.0, 5.0));
        assert!(!Operator::Equal.evaluate(5.0, 5.1));
    }

    #[test]
    fn parses_full_config_document() {
        let toml_src = r#"
            [[modbus]]
            name = "line1"
            address = "127.0.0.1:502"
            registers = [
                { name = "temp", address = 0, topic = "factory/line1/temp" }
            ]

            [[alerts]]
            name = "overheat"
            topic = "factory/line1/temp"
            field = "value"
            operator = "greater_than"
            threshold = 80.0
            publish_topic = "alerts/line1/overheat"
        "#;

        let config: AppConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(config.modbus.len(), 1);
        assert_eq!(config.modbus[0].slave_id, 1);
        assert_eq!(config.modbus[0].registers[0].quantity, 1);
        assert_eq!(config.alerts.len(), 1);
        assert_eq!(config.alerts[0].operator, Operator::GreaterThan);
    }
}
