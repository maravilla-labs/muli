// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Registry operation metrics.

use std::sync::LazyLock;
use std::time::Duration;

use prometheus::{
    HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, register_histogram_vec,
    register_int_counter, register_int_counter_vec, register_int_gauge_vec,
};

/// Prometheus metrics for registry operations.
#[derive(Clone)]
pub struct RegistryMetrics {
    push_total: IntCounterVec,
    pull_total: IntCounterVec,
    upload_total: IntCounterVec,
    auth_failures_total: IntCounterVec,
    cache_hits_total: IntCounter,
    cache_misses_total: IntCounter,
    request_duration_seconds: HistogramVec,
    blob_size_bytes: HistogramVec,
    storage_bytes: IntGaugeVec,
    blobs_total: IntGaugeVec,
    manifests_total: IntGaugeVec,
    npm_publish_total: IntCounterVec,
    npm_download_total: IntCounterVec,
    cargo_publish_total: IntCounterVec,
    cargo_download_total: IntCounterVec,
    maven_publish_total: IntCounterVec,
    maven_download_total: IntCounterVec,
}

/// Singleton metrics instance — Prometheus global registry only allows one
/// registration per metric name, so we create all counters/gauges/histograms
/// exactly once and clone the handle on subsequent calls.
static GLOBAL_METRICS: LazyLock<RegistryMetrics> = LazyLock::new(RegistryMetrics::init);

impl RegistryMetrics {
    /// Return a (cheap) clone of the globally registered metrics.
    pub fn new() -> Self {
        GLOBAL_METRICS.clone()
    }

    /// One-time initialisation — called by `LazyLock`.
    fn init() -> Self {
        let push_total = register_int_counter_vec!(
            "registry_push_total",
            "Total number of manifest pushes",
            &["tenant_id"]
        )
        .expect("failed to register registry_push_total");

        let pull_total = register_int_counter_vec!(
            "registry_pull_total",
            "Total number of manifest/blob pulls",
            &["tenant_id"]
        )
        .expect("failed to register registry_pull_total");

        let upload_total = register_int_counter_vec!(
            "registry_upload_total",
            "Total number of blob uploads",
            &["tenant_id"]
        )
        .expect("failed to register registry_upload_total");

        let auth_failures_total = register_int_counter_vec!(
            "registry_auth_failures_total",
            "Total authentication failures",
            &["tenant_id", "reason"]
        )
        .expect("failed to register registry_auth_failures_total");

        let cache_hits_total =
            register_int_counter!("registry_cache_hits_total", "Total proxy cache hits")
                .expect("failed to register registry_cache_hits_total");

        let cache_misses_total =
            register_int_counter!("registry_cache_misses_total", "Total proxy cache misses")
                .expect("failed to register registry_cache_misses_total");

        let request_duration_seconds = register_histogram_vec!(
            "registry_request_duration_seconds",
            "Request duration in seconds",
            &["method", "endpoint"],
            vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
            ]
        )
        .expect("failed to register registry_request_duration_seconds");

        let blob_size_bytes = register_histogram_vec!(
            "registry_blob_size_bytes",
            "Blob sizes in bytes",
            &["tenant_id"],
            prometheus::exponential_buckets(1024.0, 4.0, 10).unwrap()
        )
        .expect("failed to register registry_blob_size_bytes");

        let storage_bytes = register_int_gauge_vec!(
            "registry_storage_bytes",
            "Current storage usage in bytes per tenant",
            &["tenant_id"]
        )
        .expect("failed to register registry_storage_bytes");

        let blobs_total = register_int_gauge_vec!(
            "registry_blobs_total",
            "Number of blobs per tenant",
            &["tenant_id"]
        )
        .expect("failed to register registry_blobs_total");

        let manifests_total = register_int_gauge_vec!(
            "registry_manifests_total",
            "Number of manifests per tenant",
            &["tenant_id"]
        )
        .expect("failed to register registry_manifests_total");

        let npm_publish_total = register_int_counter_vec!(
            "registry_npm_publish_total",
            "Total number of npm package publishes",
            &["tenant_id"]
        )
        .expect("failed to register registry_npm_publish_total");

        let npm_download_total = register_int_counter_vec!(
            "registry_npm_download_total",
            "Total number of npm tarball downloads",
            &["tenant_id"]
        )
        .expect("failed to register registry_npm_download_total");

        let cargo_publish_total = register_int_counter_vec!(
            "registry_cargo_publish_total",
            "Total number of cargo crate publishes",
            &["tenant_id"]
        )
        .expect("failed to register registry_cargo_publish_total");

        let cargo_download_total = register_int_counter_vec!(
            "registry_cargo_download_total",
            "Total number of cargo crate downloads",
            &["tenant_id"]
        )
        .expect("failed to register registry_cargo_download_total");

        let maven_publish_total = register_int_counter_vec!(
            "registry_maven_publish_total",
            "Total number of maven artifact publishes",
            &["tenant_id"]
        )
        .expect("failed to register registry_maven_publish_total");

        let maven_download_total = register_int_counter_vec!(
            "registry_maven_download_total",
            "Total number of maven artifact downloads",
            &["tenant_id"]
        )
        .expect("failed to register registry_maven_download_total");

        Self {
            push_total,
            pull_total,
            upload_total,
            auth_failures_total,
            cache_hits_total,
            cache_misses_total,
            request_duration_seconds,
            blob_size_bytes,
            storage_bytes,
            blobs_total,
            manifests_total,
            npm_publish_total,
            npm_download_total,
            cargo_publish_total,
            cargo_download_total,
            maven_publish_total,
            maven_download_total,
        }
    }

    pub fn record_push(&self, tenant_id: &str) {
        self.push_total.with_label_values(&[tenant_id]).inc();
    }

    pub fn record_pull(&self, tenant_id: &str) {
        self.pull_total.with_label_values(&[tenant_id]).inc();
    }

    pub fn record_upload(&self, tenant_id: &str) {
        self.upload_total.with_label_values(&[tenant_id]).inc();
    }

    pub fn record_auth_failure(&self, tenant_id: &str, reason: &str) {
        self.auth_failures_total
            .with_label_values(&[tenant_id, reason])
            .inc();
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits_total.inc();
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses_total.inc();
    }

    pub fn observe_request_duration(&self, method: &str, endpoint: &str, duration: Duration) {
        self.request_duration_seconds
            .with_label_values(&[method, endpoint])
            .observe(duration.as_secs_f64());
    }

    pub fn observe_blob_size(&self, tenant_id: &str, size: u64) {
        self.blob_size_bytes
            .with_label_values(&[tenant_id])
            .observe(size as f64);
    }

    pub fn set_storage_usage(&self, tenant_id: &str, bytes: u64, blobs: u64, manifests: u64) {
        self.storage_bytes
            .with_label_values(&[tenant_id])
            .set(bytes as i64);
        self.blobs_total
            .with_label_values(&[tenant_id])
            .set(blobs as i64);
        self.manifests_total
            .with_label_values(&[tenant_id])
            .set(manifests as i64);
    }

    pub fn record_npm_publish(&self, tenant_id: &str) {
        self.npm_publish_total.with_label_values(&[tenant_id]).inc();
    }

    pub fn record_npm_download(&self, tenant_id: &str) {
        self.npm_download_total
            .with_label_values(&[tenant_id])
            .inc();
    }

    pub fn record_cargo_publish(&self, tenant_id: &str) {
        self.cargo_publish_total
            .with_label_values(&[tenant_id])
            .inc();
    }

    pub fn record_cargo_download(&self, tenant_id: &str) {
        self.cargo_download_total
            .with_label_values(&[tenant_id])
            .inc();
    }

    pub fn record_maven_publish(&self, tenant_id: &str) {
        self.maven_publish_total
            .with_label_values(&[tenant_id])
            .inc();
    }

    pub fn record_maven_download(&self, tenant_id: &str) {
        self.maven_download_total
            .with_label_values(&[tenant_id])
            .inc();
    }
}

impl Default for RegistryMetrics {
    fn default() -> Self {
        Self::new()
    }
}
