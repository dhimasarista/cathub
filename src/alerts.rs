use crate::config::AlertRule;
use dashmap::DashMap;
use rumqttd::local::LinkTx;
use serde_json::{json, Value};
use tracing::{error, info, warn};

/// A rule crossing its threshold on a specific topic.
pub struct TriggeredAlert {
    pub rule_name: String,
    pub topic: String,
    pub value: f64,
    pub threshold: f64,
    pub publish_topic: Option<String>,
    pub webhook_url: Option<String>,
}

/// Evaluates configured threshold rules against ingested payloads.
///
/// Firing is edge-triggered per (rule, topic): an alert is only produced on
/// the transition into the firing state, not on every message while it
/// remains past the threshold, so a webhook or MQTT alert topic doesn't get
/// flooded for as long as a sensor stays hot.
pub struct AlertEngine {
    rules: Vec<AlertRule>,
    firing: DashMap<(String, String), bool>,
}

impl AlertEngine {
    pub fn new(rules: Vec<AlertRule>) -> Self {
        Self {
            rules,
            firing: DashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    #[allow(dead_code)] // pairs with `len()` per clippy::len_without_is_empty
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn evaluate(&self, topic: &str, payload: &Value) -> Vec<TriggeredAlert> {
        let mut triggered = Vec::new();

        for rule in &self.rules {
            if rule.topic != topic {
                continue;
            }

            let Some(value) = payload.get(rule.field.as_str()).and_then(Value::as_f64) else {
                continue;
            };

            let is_firing = rule.operator.evaluate(value, rule.threshold);
            let key = (rule.name.clone(), topic.to_string());
            let was_firing = self.firing.get(&key).map(|v| *v).unwrap_or(false);
            self.firing.insert(key, is_firing);

            if is_firing && !was_firing {
                triggered.push(TriggeredAlert {
                    rule_name: rule.name.clone(),
                    topic: topic.to_string(),
                    value,
                    threshold: rule.threshold,
                    publish_topic: rule.publish_topic.clone(),
                    webhook_url: rule.webhook_url.clone(),
                });
            }
        }

        triggered
    }
}

/// Dispatches a triggered alert: republish to MQTT and/or POST a webhook.
/// Neither destination blocks the ingestion loop that calls this function.
pub fn dispatch(alert: &TriggeredAlert, mqtt_tx: &mut LinkTx) {
    info!(
        rule = %alert.rule_name,
        topic = %alert.topic,
        value = alert.value,
        threshold = alert.threshold,
        "Alert triggered"
    );

    if let Some(publish_topic) = &alert.publish_topic {
        let body = json!({
            "rule": alert.rule_name,
            "topic": alert.topic,
            "value": alert.value,
            "threshold": alert.threshold,
        })
        .to_string();

        if let Err(e) = mqtt_tx.publish(publish_topic.clone(), body) {
            warn!(error = ?e, topic = %publish_topic, "Failed to republish alert to MQTT");
        }
    }

    if let Some(webhook_url) = alert.webhook_url.clone() {
        let rule_name = alert.rule_name.clone();
        let topic = alert.topic.clone();
        let value = alert.value;
        let threshold = alert.threshold;

        tokio::task::spawn_blocking(move || {
            let body = json!({
                "rule": rule_name,
                "topic": topic,
                "value": value,
                "threshold": threshold,
            })
            .to_string();

            let result = ureq::post(&webhook_url)
                .config()
                .timeout_global(Some(std::time::Duration::from_secs(10)))
                .build()
                .header("Content-Type", "application/json")
                .send(body);

            if let Err(e) = result {
                error!(error = ?e, url = %redact_credentials(&webhook_url), "Webhook dispatch failed");
            }
        });
    }
}

/// Strips `user:pass@` userinfo from a URL before it's logged, so
/// credentials an operator embedded in `webhook_url` don't end up in
/// application logs on every failed delivery.
fn redact_credentials(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let (scheme, rest) = url.split_at(scheme_end + 3);
    match rest.find('@') {
        Some(at) => format!("{scheme}***@{}", &rest[at + 1..]),
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Operator;
    use serde_json::json;

    #[test]
    fn redacts_userinfo_from_url() {
        assert_eq!(
            redact_credentials("https://user:pass@example.com/hook"),
            "https://***@example.com/hook"
        );
    }

    #[test]
    fn leaves_url_without_userinfo_unchanged() {
        assert_eq!(
            redact_credentials("https://example.com/hook"),
            "https://example.com/hook"
        );
    }

    fn rule(name: &str, topic: &str, threshold: f64) -> AlertRule {
        AlertRule {
            name: name.to_string(),
            topic: topic.to_string(),
            field: "value".to_string(),
            operator: Operator::GreaterThan,
            threshold,
            publish_topic: None,
            webhook_url: None,
        }
    }

    #[test]
    fn fires_once_on_transition_into_threshold() {
        let engine = AlertEngine::new(vec![rule("overheat", "sensors/temp", 80.0)]);

        let below = json!({"value": 70.0});
        let above = json!({"value": 90.0});

        assert!(engine.evaluate("sensors/temp", &below).is_empty());
        assert_eq!(engine.evaluate("sensors/temp", &above).len(), 1);
        // Still above threshold on the next reading: no repeat alert.
        assert!(engine.evaluate("sensors/temp", &above).is_empty());
    }

    #[test]
    fn fires_again_after_dropping_back_below_threshold() {
        let engine = AlertEngine::new(vec![rule("overheat", "sensors/temp", 80.0)]);
        let above = json!({"value": 90.0});
        let below = json!({"value": 50.0});

        assert_eq!(engine.evaluate("sensors/temp", &above).len(), 1);
        assert!(engine.evaluate("sensors/temp", &below).is_empty());
        assert_eq!(engine.evaluate("sensors/temp", &above).len(), 1);
    }

    #[test]
    fn ignores_topics_that_do_not_match_any_rule() {
        let engine = AlertEngine::new(vec![rule("overheat", "sensors/temp", 80.0)]);
        let result = engine.evaluate("sensors/other", &json!({"value": 999.0}));
        assert!(result.is_empty());
    }

    #[test]
    fn ignores_payloads_missing_the_configured_field() {
        let engine = AlertEngine::new(vec![rule("overheat", "sensors/temp", 80.0)]);
        let result = engine.evaluate("sensors/temp", &json!({"other_field": 999.0}));
        assert!(result.is_empty());
    }
}
