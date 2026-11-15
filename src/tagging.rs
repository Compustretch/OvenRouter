// src/tagging.rs
//
// Post-routing task tagging, entirely off the request path.
//
// The handler calls TagQueue::enqueue() once the routing decision is made (and, if the
// tagger should see the answer, once the downstream response is in hand). enqueue()
// never blocks and never fails the request: when the queue is full it bumps a counter
// and drops the job.
//
// A background pipeline invokes the task classifier, a separate model from the router
// tiers, and appends one JSON line per request to a JSONL file. Records join to
// logs/routing_capture.jsonl on request_id.
//
// TagInput carries the prompt and, optionally, the response. The routing decision is
// not part of it, so the tagger's labels are independent of router behavior and
// P(jev | category) measures the category rather than the router.

#![allow(dead_code)] // remove once main.rs and service.rs consume this module

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tracing::warn;

use crate::pipeline::task::Task;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub estimated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CategoryScore {
    pub category_id: String,
    pub score: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UntaggedReason {
    Timeout,
    Error,
    Dropped,
    SampledOut,
    WriterDead,
}

/// Verdict returned strictly by the tagger model (excludes pipeline failures/skips).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TaggerVerdict {
    Tagged {
        primary: CategoryScore,
        #[serde(default)]
        alternatives: Vec<CategoryScore>,
    },
    NoMatch {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nearest: Option<CategoryScore>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        embedding: Option<Vec<f32>>,
    },
}

/// Final record status owned and populated by the pipeline wrapper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TagResult {
    Tagged {
        primary: CategoryScore,
        #[serde(default)]
        alternatives: Vec<CategoryScore>,
    },
    NoMatch {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nearest: Option<CategoryScore>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        embedding: Option<Vec<f32>>,
    },
    Untagged {
        reason: UntaggedReason,
    },
}

impl From<TaggerVerdict> for TagResult {
    
    fn from(v: TaggerVerdict) -> Self {
        match v {
            TaggerVerdict::Tagged {
                primary,
                alternatives,
            } => TagResult::Tagged {
                primary,
                alternatives,
            },
            TaggerVerdict::NoMatch { nearest, embedding } => {
                TagResult::NoMatch { nearest, embedding }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagRecord {
    pub request_id: String,
    pub timestamp: DateTime<Utc>,
    pub taxonomy_version: String,
    pub tagger_model: String,
    pub tagger_version: String,
    /// The task whose decision is represented by this record.
    #[serde(default = "default_task_id")]
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolving_tier: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolving_tier_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolving_tier_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_model_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decision_scores: Vec<CategoryScore>,
    pub latency_ms: f64,
    pub queue_wait_ms: f64,
    pub result: TagResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tagger_usage: Option<TokenUsage>,
}

fn default_task_id() -> String { "tag".to_string() }

// ---------------------------------------------------------------------------
// Tagger contract
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TagInput {
    pub request_id: String,
    pub prompt: String,
    pub response: Option<String>,
}

#[derive(Debug)]
pub enum TagError {
    Timeout,
    Failed(String),
}

/// Provenance for a decision computed by the cascade rather than a legacy tagger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDecisionMetadata {
    pub task_id: String,
    pub output_kind: String,
    pub resolving_tier: Option<u8>,
    pub resolving_tier_name: Option<String>,
    pub resolving_tier_id: Option<String>,
    pub model_id: Option<String>,
    pub scores: Vec<CategoryScore>,
}

pub struct TaggerOutput {
    pub verdict: TaggerVerdict,
    pub usage: Option<TokenUsage>,
    pub decision: Option<TaskDecisionMetadata>,
}

pub trait Tagger: Send + Sync + 'static {
    fn model(&self) -> &str;
    fn version(&self) -> &str;
    fn taxonomy_version(&self) -> &str;
    // fn tag(&self, input: &TagInput) -> impl Future<Output = Result<TaggerOutput, TagError>> + Send;
    // fn tag(&self, _: &TagInput) -> impl std::future::Future<Output = Result<TaggerOutput, TagError>> + Send;
    fn tag(&self, _: &TagInput) -> impl ::std::future::Future<Output = Result<TaggerOutput, TagError>> + Send;
}

pub const V1_DOMAINS: [&str; 7] = [
    "engineering",
    "company_knowledge_base",
    "help_desk",
    "customer_support",
    "web_search",
    "other_company_task",
    "other_general_request",
];

#[derive(Debug, Clone)]
pub struct OvenV1Tagger {
    client: reqwest::Client,
    base_url: String,
    model: String,
    /// Closed-set labels and their discriminating criteria. Configured task
    /// taxonomies replace the old broad-domain V1 labels.
    categories: Vec<(String, String)>,
    taxonomy_version: String,
    tagger_id: String,
}

impl OvenV1Tagger {
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
            model: model.into(),
            categories: V1_DOMAINS.iter().map(|id| ((*id).to_string(), (*id).to_string())).collect(),
            taxonomy_version: "v1.0".into(),
            tagger_id: "oven-domain-tagger-v1".into(),
        }
    }

    /// Build a taxonomy-aware tagger from the configured [task.tag] contract.
    /// The taxonomy labels/criteria have already been loaded and validated by
    /// pipeline::config, so this uses exactly the leaf set configured for the task.
    pub fn for_task(
        base_url: impl Into<String>,
        model: impl Into<String>,
        task: &Task,
    ) -> Result<Self, String> {
        let labels = task.labels.as_ref()
            .filter(|labels| !labels.is_empty())
            .ok_or_else(|| format!("task.{} has no configured taxonomy labels", task.name))?;
        let categories: Vec<_> = labels.iter().map(|id| {
            let criterion = task.label_metadata.iter()
                .find(|metadata| metadata.id == *id)
                .map(|metadata| metadata.classifier_criterion.clone())
                .filter(|criterion| !criterion.trim().is_empty())
                .unwrap_or_else(|| id.clone());
            (id.clone(), criterion)
        }).collect();
        if categories.len() != categories.iter().map(|(id, _)| id).collect::<std::collections::BTreeSet<_>>().len() {
            return Err(format!("task.{} contains duplicate taxonomy labels", task.name));
        }
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
            model: model.into(),
            categories,
            taxonomy_version: task.taxonomy_version.clone().unwrap_or_else(|| "unknown".into()),
            tagger_id: format!("{}-taxonomy-tagger", task.name),
        })
    }
}

impl Tagger for OvenV1Tagger {
    fn model(&self) -> &str {
        &self.tagger_id
    }

    fn version(&self) -> &str {
        "1.0.0"
    }

    fn taxonomy_version(&self) -> &str {
        &self.taxonomy_version
    }

    fn tag(&self, input: &TagInput) -> impl Future<Output = Result<TaggerOutput, TagError>> + Send {
        let client = self.client.clone();
        let base_url = self.base_url.clone();
        let model = self.model.clone();
        let categories = self.categories.clone();
        let prompt = input.prompt.clone();
        let request_id = input.request_id.clone();
        let response = input.response.clone().unwrap_or_default();

        async move {
            let taxonomy = categories.iter()
                .map(|(id, criterion)| format!("- {id}: {criterion}"))
                .collect::<Vec<_>>()
                .join("\n");
            let request_body = serde_json::json!({
                "model": model,
                "stream": false,
                "options": { "temperature": 0.0 },
                "messages": [
                    {
                        "role": "system",
                        "content": format!(
                            "Classify the task pursued by the current request using the configured closed-set taxonomy. Return exactly one valid JSON object with the shape {{\"status\":\"tagged\",\"primary\":{{\"category_id\":\"<one configured leaf id>\",\"score\":1.0}},\"alternatives\":[]}}. Allowed taxonomy leaves and classifier criteria:\n{}. Choose exactly one leaf ID, apply each classifier criterion as the discriminating rule, and prefer the most specific defensible leaf. Do not return broad domains, group names, or labels outside this taxonomy. Task category is independent of Jev eligibility. Do not add explanatory text or markdown fences.",
                            taxonomy
                        )
                    },
                    {
                        "role": "user",
                        "content": if response.is_empty() {
                            format!("Prompt:\n{}", prompt)
                        } else {
                            format!("Prompt:\n{}\n\nResponse:\n{}", prompt, response)
                        }
                    }
                ]
            });

            let url = format!("{}/api/chat", base_url.trim_end_matches('/'));
            let response = client
                .post(url)
                .header("X-Request-ID", &request_id)
                .json(&request_body)
                .send()
                .await
                .map_err(|error| TagError::Failed(format!("ollama request failed: {error}")))?;

            if !response.status().is_success() {
                return Err(TagError::Failed(format!(
                    "ollama request returned {}",
                    response.status()
                )));
            }

            let payload: serde_json::Value = response.json().await.map_err(|error| {
                TagError::Failed(format!("ollama response parse failed: {error}"))
            })?;

            let content = payload
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    TagError::Failed("ollama response missing message.content".to_string())
                })?;

            let candidate = content
                .trim()
                .strip_prefix("```json")
                .unwrap_or(content.trim())
                .strip_suffix("```")
                .unwrap_or(content.trim())
                .trim();

            let parsed: serde_json::Value = serde_json::from_str(candidate)
                .map_err(|error| TagError::Failed(format!("invalid JSON verdict: {error}")))?;

            let category_id = parsed
                .get("primary")
                .and_then(|primary| primary.get("category_id"))
                .and_then(serde_json::Value::as_str)
                .filter(|id| categories.iter().any(|(allowed, _)| allowed.as_str() == *id))
                .ok_or_else(|| {
                    TagError::Failed("tagger did not return a valid category id".to_string())
                })?;

            let score = parsed
                .get("primary")
                .and_then(|primary| primary.get("score"))
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(1.0) as f32;

            Ok(TaggerOutput {
                verdict: TaggerVerdict::Tagged {
                    primary: CategoryScore {
                        category_id: category_id.to_string(),
                        score,
                    },
                    alternatives: vec![],
                },
                usage: Some(TokenUsage {
                    input_tokens: 0,
                    output_tokens: 0,
                    estimated: true,
                }),
                decision: None,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------------

struct TagJob {
    input: TagInput,
    enqueued_at: Instant,
}

#[derive(Clone)]
pub struct TagQueue {
    tx: mpsc::Sender<TagJob>,
    dropped: Arc<AtomicU64>,
}

impl TagQueue {
    pub fn enqueue(&self, input: TagInput) {
        let job = TagJob {
            input,
            enqueued_at: Instant::now(),
        };
        if self.tx.try_send(job).is_err() {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_power_of_two() {
                warn!(dropped_tag_jobs = n, "tag queue unavailable; dropping job");
            }
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TagConfig {
    pub base_path: PathBuf,
    pub clickhouse_url: Option<String>,
    pub queue_capacity: usize,
    pub concurrency: usize,
    pub call_timeout: Duration,
    pub max_age: Duration,
    pub sample_prob: f32,
}

#[derive(Clone, Copy)]
pub enum ClickHouseRawTable {
    RouteRecords,
    TaskTags,
}

impl Default for TagConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("logs/task_tags.jsonl"),
            clickhouse_url: None,
            queue_capacity: 1024,
            concurrency: 4,
            call_timeout: Duration::from_secs(5),
            max_age: Duration::from_secs(60),
            sample_prob: 1.0,
        }
    }
}

const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;

pub fn is_sampled(request_id: &str, prob: f32) -> bool {
    if prob >= 1.0 {
        return true;
    }
    if prob <= 0.0 {
        return false;
    }
    let digest = Sha256::digest(request_id.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    (u64::from_be_bytes(bytes) as f64 / TWO_POW_64) < f64::from(prob)
}

async fn process<T: Tagger>(tagger: &T, cfg: &TagConfig, job: TagJob) -> TagRecord {
    let queue_wait = job.enqueued_at.elapsed();
    let timestamp = Utc::now();
    let started = Instant::now();

    let (result, tagger_usage, task_decision) = if !is_sampled(&job.input.request_id, cfg.sample_prob) {
        (
            TagResult::Untagged {
                reason: UntaggedReason::SampledOut,
            },
            None,
            None,
        )
    } else if queue_wait > cfg.max_age {
        (
            TagResult::Untagged {
                reason: UntaggedReason::Dropped,
            },
            None,
            None,
        )
    } else {
        match tokio::time::timeout(cfg.call_timeout, tagger.tag(&job.input)).await {
            Err(_) | Ok(Err(TagError::Timeout)) => (
                TagResult::Untagged {
                    reason: UntaggedReason::Timeout,
                },
                None,
                None,
            ),
            Ok(Err(TagError::Failed(error))) => {
                warn!(request_id = %job.input.request_id, error = %error, "task tagger failed");
                (
                    TagResult::Untagged {
                        reason: UntaggedReason::Error,
                    },
                    None,
                    None,
                )
            }
            Ok(Ok(output)) => (output.verdict.into(), output.usage, output.decision),
        }
    };

    TagRecord {
        request_id: job.input.request_id,
        timestamp,
        taxonomy_version: tagger.taxonomy_version().to_string(),
        tagger_model: tagger.model().to_string(),
        tagger_version: tagger.version().to_string(),
        task_id: task_decision.as_ref().map(|decision| decision.task_id.clone()).unwrap_or_else(default_task_id),
        output_kind: task_decision.as_ref().map(|decision| decision.output_kind.clone()),
        resolving_tier: task_decision.as_ref().and_then(|decision| decision.resolving_tier),
        resolving_tier_name: task_decision.as_ref().and_then(|decision| decision.resolving_tier_name.clone()),
        resolving_tier_id: task_decision.as_ref().and_then(|decision| decision.resolving_tier_id.clone()),
        decision_model_id: task_decision.as_ref().and_then(|decision| decision.model_id.clone()),
        decision_scores: task_decision.as_ref().map(|decision| decision.scores.clone()).unwrap_or_default(),
        latency_ms: started.elapsed().as_secs_f64() * 1000.0,
        queue_wait_ms: queue_wait.as_secs_f64() * 1000.0,
        result,
        tagger_usage,
    }
}

pub fn spawn<T: Tagger>(
    tagger: Arc<T>,
    cfg: TagConfig,
) -> (TagQueue, JoinHandle<std::io::Result<()>>) {
    let capacity = cfg.queue_capacity.max(1);
    let concurrency = cfg.concurrency.max(1);
    let cfg = Arc::new(cfg);

    let (job_tx, mut job_rx) = mpsc::channel::<TagJob>(capacity);
    let (rec_tx, mut rec_rx) = mpsc::channel::<TagRecord>(capacity);
    let queue = TagQueue {
        tx: job_tx,
        dropped: Arc::new(AtomicU64::new(0)),
    };

    let writer_path = cfg.base_path.clone();
    let clickhouse_url = cfg.clickhouse_url.clone();
    let writer = tokio::spawn(async move {
        let mut current_date = String::new();
        let mut file: Option<tokio::fs::File> = None;
        let clickhouse_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(std::io::Error::other)?;

        async fn get_file(base: &Path, date_str: &str) -> std::io::Result<tokio::fs::File> {
            let path = if let Some(parent) = base.parent() {
                tokio::fs::create_dir_all(parent).await?;
                parent.join(format!("task_tags.{date_str}.jsonl"))
            } else {
                PathBuf::from(format!("task_tags.{date_str}.jsonl"))
            };
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await
        }

        let mut flush = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                record = rec_rx.recv() => {
                    let Some(record) = record else { break };
                    let date_str = record.timestamp.format("%Y-%m-%d").to_string();
                    if date_str != current_date || file.is_none() {
                        if let Some(mut f) = file.take() {
                            f.flush().await?;
                        }
                        current_date = date_str;
                        file = Some(get_file(&writer_path, &current_date).await?);
                    }
                    if let Some(f) = file.as_mut() {
                        let mut line = serde_json::to_string(&record)?;
                        line.push('\n');
                        f.write_all(line.as_bytes()).await?;
                        if let Some(url) = &clickhouse_url {
                            if let Err(error) = insert_clickhouse_raw_line(
                                &clickhouse_client,
                                url,
                                ClickHouseRawTable::TaskTags,
                                line.trim_end(),
                            )
                            .await
                            {
                                warn!(
                                    request_id = %record.request_id,
                                    error = %error,
                                    "failed to insert task tag into ClickHouse; retained in local JSONL spool"
                                );
                            }
                        }
                    }
                }
                _ = flush.tick() => {
                    if let Some(f) = file.as_mut() {
                        f.flush().await?;
                    }
                }
            }
        }
        if let Some(mut f) = file {
            f.flush().await?;
        }
        Ok(())
    });

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let dispatcher = tokio::spawn(async move {
        while let Some(job) = job_rx.recv().await {
            let permit = Arc::clone(&semaphore)
                .acquire_owned()
                .await
                .expect("semaphore is never closed");

            let tagger = Arc::clone(&tagger);
            let cfg = Arc::clone(&cfg);
            let rec_tx = rec_tx.clone();
            tokio::spawn(async move {
                let record = process(tagger.as_ref(), &cfg, job).await;
                if rec_tx.send(record).await.is_err() {
                    warn!("task tag writer is unavailable");
                }
                drop(permit);
            });
        }
        let _ = semaphore.acquire_many(concurrency as u32).await;
    });

    let handle = tokio::spawn(async move {
        let _ = dispatcher.await;
        writer.await.map_err(std::io::Error::other)?
    });

    (queue, handle)
}

pub async fn insert_clickhouse_raw_line(
    client: &reqwest::Client,
    base_url: &str,
    table: ClickHouseRawTable,
    line: &str,
) -> Result<(), String> {
    let table = match table {
        ClickHouseRawTable::RouteRecords => "route_records_raw",
        ClickHouseRawTable::TaskTags => "task_tags_raw",
    };
    let query = format!("INSERT INTO llm_observability.{table} (line) FORMAT JSONEachRow");
    let url = format!("{}/", base_url.trim_end_matches('/'));
    let body = serde_json::json!({ "line": line });
    let mut last_error = None;
    for attempt in 0..3 {
        match client
            .post(&url)
            .query(&[("query", &query)])
            .json(&body)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => {
                let status = response.status();
                let detail = match response.text().await {
                    Ok(detail) => detail,
                    Err(error) => format!("failed to read response body: {error}"),
                };
                last_error = Some(format!("ClickHouse returned {status}: {detail}"));
            }
            Err(error) => last_error = Some(format!("ClickHouse request failed: {error}")),
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
        }
    }
    Err(last_error.unwrap_or_else(|| "ClickHouse insert failed without an error".into()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    struct Fixed {
        delay: Duration,
        verdict: TaggerVerdict,
    }

    impl Tagger for Fixed {
        fn model(&self) -> &str {
            "fake-tagger"
        }
        fn version(&self) -> &str {
            "t0"
        }
        fn taxonomy_version(&self) -> &str {
            "tax-1"
        }
        fn tag(
            &self,
            _input: &TagInput,
        ) -> impl Future<Output = Result<TaggerOutput, TagError>> + Send {
            let delay = self.delay;
            let verdict = self.verdict.clone();
            async move {
                tokio::time::sleep(delay).await;
                Ok(TaggerOutput {
                    verdict,
                    usage: Some(TokenUsage {
                        input_tokens: 10,
                        output_tokens: 2,
                        estimated: false,
                    }),
                    decision: None,
                })
            }
        }
    }

    fn tagged() -> TaggerVerdict {
        TaggerVerdict::Tagged {
            primary: CategoryScore {
                category_id: "c7".into(),
                score: 0.9,
            },
            alternatives: vec![],
        }
    }

    fn input(id: &str) -> TagInput {
        TagInput {
            request_id: id.into(),
            prompt: "p".into(),
            response: None,
        }
    }

    fn temp_path(name: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("tagging-{}-{name}-{unique}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        dir.join("task_tags.jsonl")
    }

    fn cfg(base_path: PathBuf) -> TagConfig {
        TagConfig {
            base_path,
            ..TagConfig::default()
        }
    }

    fn read_records(path: &Path) -> Vec<TagRecord> {
        let today = Utc::now().format("%Y-%m-%d").to_string();
        let actual_path = path
            .parent()
            .unwrap()
            .join(format!("task_tags.{today}.jsonl"));
        std::fs::read_to_string(actual_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn by_id<'a>(records: &'a [TagRecord], id: &str) -> &'a TagRecord {
        records.iter().find(|r| r.request_id == id).unwrap()
    }

    #[test]
    fn task_taxonomy_replaces_the_broad_domain_leaf_set() {
        let task = Task {
            name: "tag".into(),
            cascade: Default::default(),
            output_kind: crate::pipeline::task::OutputKind::Multiclass,
            labels: Some(vec!["account_pw".into(), "nw_vpn".into()]),
            label_metadata: vec![
                crate::pipeline::task::TaskLabelMetadata {
                    id: "account_pw".into(),
                    name: "Password reset".into(),
                    description: "Reset a password or unlock an account".into(),
                    classifier_criterion: "Reset credentials or unlock an account".into(),
                    group_id: None,
                },
                crate::pipeline::task::TaskLabelMetadata {
                    id: "nw_vpn".into(),
                    name: "VPN connectivity".into(),
                    description: "Network and VPN access".into(),
                    classifier_criterion: "Diagnose VPN or network connectivity".into(),
                    group_id: None,
                },
            ],
            taxonomy_version: Some("1.1.0".into()),
            fallback: crate::pipeline::task::FallbackPolicy::Defer,
        };
        let tagger = OvenV1Tagger::for_task(
            "http://127.0.0.1:11434",
            "qwen3:1.7b",
            &task,
        ).unwrap();

        assert_eq!(tagger.taxonomy_version(), "1.1.0");
        assert_eq!(tagger.model(), "tag-taxonomy-tagger");
        assert_eq!(
            tagger.categories,
            vec![
                ("account_pw".into(), "Reset credentials or unlock an account".into()),
                ("nw_vpn".into(), "Diagnose VPN or network connectivity".into()),
            ]
        );
    }

    #[tokio::test]
    async fn clickhouse_sink_inserts_raw_json_each_row() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            let mut content_length = None;
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                request.extend_from_slice(&buffer[..read]);
                if content_length.is_none() {
                    if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..header_end]);
                        content_length = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        });
                    }
                }
                if let Some(content_length) = content_length {
                    let header_end = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                    if request.len() >= header_end + content_length {
                        break;
                    }
                }
            }
            let _ = request_tx.send(String::from_utf8_lossy(&request).into_owned());
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
        });

        insert_clickhouse_raw_line(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            ClickHouseRawTable::TaskTags,
            r#"{"request_id":"req-1","result":{"status":"untagged"}}"#,
        )
        .await
        .unwrap();

        let request = request_rx.await.unwrap();
        assert!(
            request.contains("query=INSERT+INTO+llm_observability.task_tags_raw"),
            "{request}"
        );
        assert!(request.contains(
            r#""line":"{\"request_id\":\"req-1\",\"result\":{\"status\":\"untagged\"}}""#
        ));
    }

    #[tokio::test]
    async fn clickhouse_sink_inserts_route_records_into_route_table() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            let mut content_length = None;
            loop {
                let read = stream.read(&mut buffer).await.unwrap();
                request.extend_from_slice(&buffer[..read]);
                if content_length.is_none() {
                    if let Some(header_end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&request[..header_end]);
                        content_length = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        });
                    }
                }
                if let Some(content_length) = content_length {
                    let header_end = request.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                    if request.len() >= header_end + content_length {
                        break;
                    }
                }
            }
            let _ = request_tx.send(String::from_utf8_lossy(&request).into_owned());
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await
                .unwrap();
        });

        let route_line = r#"{"request_id":"route-1","final_backend":"llm"}"#;
        insert_clickhouse_raw_line(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            ClickHouseRawTable::RouteRecords,
            route_line,
        )
        .await
        .unwrap();

        let request = request_rx.await.unwrap();
        assert!(
            request.contains("query=INSERT+INTO+llm_observability.route_records_raw"),
            "{request}"
        );
        assert!(
            request.contains(r#""line":"{\"request_id\":\"route-1\",\"final_backend\":\"llm\"}""#)
        );
    }

    #[test]
    fn no_match_with_embedding_and_untagged_serialize_distinctly() {
        let no_match = serde_json::to_string(&TagResult::NoMatch {
            nearest: None,
            embedding: Some(vec![0.1, 0.2]),
        })
        .unwrap();
        let untagged = serde_json::to_string(&TagResult::Untagged {
            reason: UntaggedReason::Timeout,
        })
        .unwrap();
        assert_eq!(no_match, r#"{"status":"no_match","embedding":[0.1,0.2]}"#);
        assert_eq!(untagged, r#"{"status":"untagged","reason":"timeout"}"#);
    }

    #[test]
    fn tag_record_json_matches_clickhouse_projection_shape() {
        let tagged = TagRecord {
            request_id: "req-1".into(),
            timestamp: Utc::now(),
            taxonomy_version: "v1.0".into(),
            tagger_model: "oven-domain-tagger-v1".into(),
            tagger_version: "1.0.0".into(),
            task_id: "tag".into(),
            output_kind: Some("multiclass".into()),
            resolving_tier: Some(3),
            resolving_tier_name: Some("modernbert".into()),
            resolving_tier_id: None,
            decision_model_id: Some("shared-encoder".into()),
            decision_scores: vec![
                CategoryScore { category_id: "engineering".into(), score: 0.91 },
                CategoryScore { category_id: "help_desk".into(), score: 0.09 },
            ],
            latency_ms: 12.5,
            queue_wait_ms: 1.2,
            result: TagResult::Tagged {
                primary: CategoryScore {
                    category_id: "engineering".into(),
                    score: 0.91,
                },
                alternatives: vec![CategoryScore {
                    category_id: "help_desk".into(),
                    score: 0.09,
                }],
            },
            tagger_usage: Some(TokenUsage {
                input_tokens: 128,
                output_tokens: 32,
                estimated: false,
            }),
        };

        let value = serde_json::to_value(&tagged).unwrap();
        assert_eq!(value["result"]["status"], "tagged");
        assert_eq!(value["result"]["primary"]["category_id"], "engineering");
        assert!((value["result"]["primary"]["score"].as_f64().unwrap() - 0.91).abs() < 1e-6);
        assert_eq!(value["tagger_usage"]["input_tokens"], 128);
        assert_eq!(value["tagger_usage"]["output_tokens"], 32);
    }

    #[tokio::test]
    async fn pipeline_writes_one_record_per_job() {
        let path = temp_path("basic");
        let tagger = Arc::new(Fixed {
            delay: Duration::ZERO,
            verdict: tagged(),
        });
        let (queue, handle) = spawn(tagger, cfg(path.clone()));
        for i in 0..3 {
            queue.enqueue(input(&format!("r{i}")));
        }
        drop(queue);
        handle.await.unwrap().unwrap();

        let records = read_records(&path);
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|r| {
            matches!(r.result, TagResult::Tagged { .. })
                && r.tagger_usage.is_some()
                && r.taxonomy_version == "tax-1"
                && r.tagger_model == "fake-tagger"
        }));
    }

    #[tokio::test]
    async fn slow_tagger_yields_untagged_timeout() {
        let path = temp_path("timeout");
        let tagger = Arc::new(Fixed {
            delay: Duration::from_millis(300),
            verdict: tagged(),
        });
        let config = TagConfig {
            call_timeout: Duration::from_millis(20),
            ..cfg(path.clone())
        };
        let (queue, handle) = spawn(tagger, config);
        queue.enqueue(input("slow"));
        drop(queue);
        handle.await.unwrap().unwrap();

        let records = read_records(&path);
        assert_eq!(
            records[0].result,
            TagResult::Untagged {
                reason: UntaggedReason::Timeout
            }
        );
        assert!(records[0].tagger_usage.is_none());
    }
    #[tokio::test]
    async fn tagger_propagates_request_id_in_http_header() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            let n = stream.read(&mut request).await.unwrap_or(0);
            let text = String::from_utf8_lossy(&request[..n]).to_ascii_lowercase();
            let request_id = text.lines()
                .find_map(|line| line.strip_prefix("x-request-id: "))
                .unwrap_or("")
                .trim().to_string();
            let _ = tx.send(request_id);
            let body = r#"{"message":{"content":"{\"status\":\"tagged\",\"primary\":{\"category_id\":\"help_desk\",\"score\":0.9},\"alternatives\":[]}"}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(), body
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });

        let request_id = "123e4567-e89b-42d3-a456-426614174000";
        let tagger = OvenV1Tagger::new(format!("http://{address}"), "test-model");
        let result = tagger.tag(&TagInput {
            request_id: request_id.into(),
            prompt: "Help me reset my password".into(),
            response: None,
        }).await.unwrap();
        assert!(matches!(result.verdict,
            TaggerVerdict::Tagged { primary, .. } if primary.category_id == "help_desk"));
        assert_eq!(rx.await.unwrap(), request_id);
    }

}
