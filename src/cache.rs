use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::warn;

/// Hard ceiling on distinct topics tracked at once. Without this, a client
/// that can publish (the embedded broker has no auth unless MQTT_USERNAME /
/// MQTT_PASSWORD are set) could grow this cache without bound by publishing
/// to a stream of unique topics. Once the limit is reached, messages for
/// topics not already tracked are dropped from the whole pipeline (cache,
/// vault, SSE, alerts) rather than just left uncached — this is a circuit
/// breaker, not graceful degradation. Real IIoT deployments have nowhere
/// near this many distinct topics.
const MAX_TRACKED_TOPICS: usize = 10_000;

/// The most recently seen value for a topic, kept in memory so dashboards
/// can read current state instantly without touching the database.
#[derive(Debug, Clone, Serialize)]
pub struct CachedValue {
    pub payload: Value,
    pub updated_at: DateTime<Utc>,
    #[serde(skip)]
    hash: u64,
}

pub enum UpdateOutcome {
    Changed,
    Unchanged,
}

/// In-memory latest-value store, keyed by MQTT topic.
///
/// `update` also acts as the dedup gate for the rest of the pipeline: a
/// payload identical (byte-for-byte) to the last one seen for its topic is
/// reported as `Unchanged` so the caller can skip broadcasting, persisting,
/// and re-evaluating alerts for it.
#[derive(Debug, Default)]
pub struct StateCache {
    values: DashMap<String, CachedValue>,
}

impl StateCache {
    pub fn new() -> Self {
        Self {
            values: DashMap::new(),
        }
    }

    pub fn update(&self, topic: &str, raw_payload: &str, payload: Value) -> UpdateOutcome {
        let mut hasher = DefaultHasher::new();
        raw_payload.hash(&mut hasher);
        let hash = hasher.finish();

        if let Some(existing) = self.values.get(topic) {
            if existing.hash == hash {
                return UpdateOutcome::Unchanged;
            }
        } else if self.values.len() >= MAX_TRACKED_TOPICS {
            warn!(
                topic = %topic,
                limit = MAX_TRACKED_TOPICS,
                "Dropping message: distinct-topic limit reached, refusing to track a new topic"
            );
            return UpdateOutcome::Unchanged;
        }

        self.values.insert(
            topic.to_string(),
            CachedValue {
                payload,
                updated_at: Utc::now(),
                hash,
            },
        );
        UpdateOutcome::Changed
    }

    pub fn get(&self, topic: &str) -> Option<CachedValue> {
        self.values.get(topic).map(|entry| entry.clone())
    }

    pub fn snapshot(&self) -> HashMap<String, CachedValue> {
        self.values
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn first_update_for_a_topic_is_always_changed() {
        let cache = StateCache::new();
        let outcome = cache.update("sensors/a", "{\"v\":1}", json!({"v": 1}));
        assert!(matches!(outcome, UpdateOutcome::Changed));
    }

    #[test]
    fn identical_payload_is_reported_unchanged() {
        let cache = StateCache::new();
        cache.update("sensors/a", "{\"v\":1}", json!({"v": 1}));
        let outcome = cache.update("sensors/a", "{\"v\":1}", json!({"v": 1}));
        assert!(matches!(outcome, UpdateOutcome::Unchanged));
    }

    #[test]
    fn different_payload_is_reported_changed_and_replaces_cached_value() {
        let cache = StateCache::new();
        cache.update("sensors/a", "{\"v\":1}", json!({"v": 1}));
        let outcome = cache.update("sensors/a", "{\"v\":2}", json!({"v": 2}));
        assert!(matches!(outcome, UpdateOutcome::Changed));
        assert_eq!(cache.get("sensors/a").unwrap().payload, json!({"v": 2}));
    }

    #[test]
    fn unknown_topic_returns_none() {
        let cache = StateCache::new();
        assert!(cache.get("missing/topic").is_none());
    }
}
