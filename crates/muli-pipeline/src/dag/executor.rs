// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! DAG executor: processes pipeline steps level-by-level, submitting each as a Job.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{error, info, warn};

use muli_core::error::Result;
use muli_core::job::model::{EnvVar, Job, JobSpec, PriorityTier};
use muli_core::job::state_machine::JobState;
use muli_core::pipeline::{
    FailureStrategy, PipelineRun, PipelineRunState, StepRun, StepRunState,
};
use muli_core::resource::limits::ResourceSpec;
use muli_core::traits::{JobStore, PipelineRunStore, StepRunStore};

use crate::dag::graph::DagGraph;
use crate::yaml::expression::{evaluate_condition, ExpressionContext};
use crate::yaml::schema::{PipelineDef, StepDef};

/// Abstraction for submitting jobs to the scheduler.
/// Implemented by the server layer to avoid coupling muli-pipeline to muli-queue.
#[async_trait]
pub trait JobSubmitter: Send + Sync {
    /// Create a Job record and enqueue it for execution. Returns the job ID.
    async fn submit(&self, job: Job) -> Result<String>;
}

/// Default polling interval when waiting for jobs to complete.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum time to wait for a single pipeline run before giving up.
const MAX_PIPELINE_DURATION: Duration = Duration::from_secs(3600); // 1 hour

/// Orchestrates pipeline execution by processing DAG levels.
pub struct DagExecutor {
    pub run_store: Arc<dyn PipelineRunStore>,
    pub step_store: Arc<dyn StepRunStore>,
    pub job_store: Arc<dyn JobStore>,
    pub job_submitter: Arc<dyn JobSubmitter>,
}

impl DagExecutor {
    pub fn new(
        run_store: Arc<dyn PipelineRunStore>,
        step_store: Arc<dyn StepRunStore>,
        job_store: Arc<dyn JobStore>,
        job_submitter: Arc<dyn JobSubmitter>,
    ) -> Self {
        Self {
            run_store,
            step_store,
            job_store,
            job_submitter,
        }
    }

    /// Execute a pipeline run end-to-end: evaluate conditions, process DAG levels,
    /// submit Jobs, wait for completion, compute final state.
    pub async fn execute(
        &self,
        run: &mut PipelineRun,
        pipeline_def: &PipelineDef,
        step_runs: &[StepRun],
        clone_url: Option<&str>,
    ) -> Result<PipelineRunState> {
        let tenant_id = &run.tenant_id;

        // Mark run as Running
        run.state = PipelineRunState::Running;
        run.started_at = Some(Utc::now());
        run.updated_at = Utc::now();
        self.run_store.update_run(run).await?;

        let expr_ctx = ExpressionContext {
            branch: run.ref_name.replace("refs/heads/", ""),
            event: trigger_event_str(&run.trigger),
            tag: None,
        };

        // Build step name → StepDef lookup.
        let step_def_map: HashMap<&str, &StepDef> = pipeline_def
            .steps
            .iter()
            .map(|s| (s.name.as_str(), s))
            .collect();

        // Collect original step names for matching matrix-expanded step_runs
        let original_names: Vec<&str> = pipeline_def.steps.iter().map(|s| s.name.as_str()).collect();

        // Build step_run_name → StepRun map, and original_name → [step_run_names] map
        let step_run_map: HashMap<&str, &StepRun> = step_runs
            .iter()
            .map(|s| (s.step_name.as_str(), s))
            .collect();

        // Map original step name → all expanded step_run names
        let mut original_to_runs: HashMap<&str, Vec<&str>> = HashMap::new();
        for sr in step_runs {
            if let Some(orig) = find_original_name(&sr.step_name, &original_names) {
                original_to_runs.entry(orig).or_default().push(&sr.step_name);
            }
        }

        // Evaluate `if` conditions — skip steps that don't match
        for sr in step_runs.iter() {
            let orig = find_original_name(&sr.step_name, &original_names);
            if let Some(def) = orig.and_then(|n| step_def_map.get(n)) {
                if let Some(ref condition) = def.condition {
                    if !evaluate_condition(condition, &expr_ctx) {
                        self.step_store
                            .update_step_state(tenant_id, &sr.id, StepRunState::Skipped)
                            .await?;
                        info!(step = %sr.step_name, "step skipped (condition not met)");
                    }
                }
            }
        }

        // Build DAG from step definitions (name → deps)
        let step_deps: Vec<(String, Vec<String>)> = pipeline_def
            .steps
            .iter()
            .map(|s| (s.name.clone(), s.needs.clone()))
            .collect();
        let dag = DagGraph::from_steps(&step_deps);
        let levels = dag.topological_levels();

        info!(
            run_id = %run.id,
            levels = levels.len(),
            total_steps = step_runs.len(),
            "executing pipeline DAG"
        );

        let mut had_failure = false;
        let mut had_stop_failure = false;
        let deadline = Utc::now() + chrono::Duration::from_std(MAX_PIPELINE_DURATION).unwrap();

        // Process each level sequentially; within a level, steps run in parallel
        for (level_idx, level) in levels.iter().enumerate() {
            if had_stop_failure {
                // A step with failure_strategy=Stop failed — cancel remaining levels
                for &orig_name in level {
                    let run_names = original_to_runs.get(orig_name).cloned().unwrap_or_default();
                    for rn in run_names {
                        if let Some(sr) = step_run_map.get(rn) {
                            let current = self.step_store.get_step(tenant_id, &sr.id).await?;
                            if let Some(s) = current {
                                if !s.state.is_terminal() {
                                    self.step_store
                                        .update_step_state(tenant_id, &sr.id, StepRunState::Cancelled)
                                        .await?;
                                }
                            }
                        }
                    }
                }
                continue;
            }

            info!(run_id = %run.id, level = level_idx, steps = ?level, "processing DAG level");

            // Collect all step_runs in this level (including matrix expansions)
            let mut level_runs: Vec<&str> = Vec::new();
            for &orig_name in level {
                if let Some(runs) = original_to_runs.get(orig_name) {
                    level_runs.extend_from_slice(runs);
                }
            }

            // Submit all ready steps in this level as Jobs
            let mut submitted: Vec<(String, String)> = Vec::new(); // (step_run_name, job_id)

            for &sr_name in &level_runs {
                let sr = match step_run_map.get(sr_name) {
                    Some(sr) => sr,
                    None => continue,
                };

                // Re-fetch current state (may have been skipped by conditions)
                let current = self.step_store.get_step(tenant_id, &sr.id).await?;
                let current_state = current.as_ref().map(|s| s.state);
                if current_state == Some(StepRunState::Skipped)
                    || current_state == Some(StepRunState::Cancelled)
                {
                    continue;
                }

                let orig_name = find_original_name(sr_name, &original_names);
                let step_def = match orig_name.and_then(|n| step_def_map.get(n)) {
                    Some(d) => *d,
                    None => continue,
                };

                // Build the Job
                let job = self.build_job(run, pipeline_def, step_def, clone_url);

                // Mark step as Running
                self.step_store
                    .update_step_state(tenant_id, &sr.id, StepRunState::Running)
                    .await?;

                // Submit to scheduler
                match self.job_submitter.submit(job).await {
                    Ok(job_id) => {
                        // Record job_id on step
                        if let Some(mut step) = self.step_store.get_step(tenant_id, &sr.id).await? {
                            step.job_id = Some(job_id.clone());
                            step.started_at = Some(Utc::now());
                            step.updated_at = Utc::now();
                            self.step_store.update_step(&step).await?;
                        }
                        submitted.push((sr_name.to_string(), job_id));
                        info!(step = %sr_name, "step job submitted");
                    }
                    Err(e) => {
                        error!(step = %sr_name, error = %e, "failed to submit step job");
                        self.step_store
                            .update_step_state(tenant_id, &sr.id, StepRunState::Failed)
                            .await?;
                        had_failure = true;
                        if sr.failure_strategy == FailureStrategy::Stop {
                            had_stop_failure = true;
                        }
                    }
                }
            }

            // Wait for all submitted jobs in this level to complete
            for (step_name, job_id) in &submitted {
                let sr = match step_run_map.get(step_name.as_str()) {
                    Some(sr) => sr,
                    None => continue,
                };

                let final_state = self.wait_for_job(job_id, deadline).await;

                let step_state = match final_state {
                    Some(JobState::Succeeded) => StepRunState::Succeeded,
                    Some(JobState::Cancelled) => StepRunState::Cancelled,
                    Some(JobState::TimedOut) => StepRunState::Failed,
                    _ => StepRunState::Failed,
                };

                // Update step with final state
                if let Some(mut step) = self.step_store.get_step(tenant_id, &sr.id).await? {
                    step.state = step_state;
                    step.finished_at = Some(Utc::now());
                    step.updated_at = Utc::now();
                    self.step_store.update_step(&step).await?;
                }

                info!(step = %step_name, state = ?step_state, "step completed");

                if step_state == StepRunState::Failed {
                    had_failure = true;
                    if sr.failure_strategy == FailureStrategy::Stop {
                        had_stop_failure = true;
                    }
                }
            }
        }

        // Compute final pipeline state
        let final_state = if had_stop_failure {
            PipelineRunState::Failed
        } else if had_failure {
            PipelineRunState::Degraded
        } else {
            PipelineRunState::Succeeded
        };

        run.state = final_state;
        run.finished_at = Some(Utc::now());
        run.updated_at = Utc::now();
        self.run_store.update_run(run).await?;

        info!(
            run_id = %run.id,
            state = ?final_state,
            "pipeline run completed"
        );

        Ok(final_state)
    }

    /// Build a Job from a pipeline step definition.
    fn build_job(
        &self,
        run: &PipelineRun,
        pipeline_def: &PipelineDef,
        step_def: &StepDef,
        clone_url: Option<&str>,
    ) -> Job {
        let branch = run.ref_name.replace("refs/heads/", "");

        // Build environment variables: pipeline globals → step env → run env_vars → built-ins
        let mut env_vars: Vec<EnvVar> = Vec::new();

        // 1. Pipeline-level env
        for (k, v) in &pipeline_def.env {
            env_vars.push(EnvVar {
                name: k.clone(),
                value: v.clone(),
            });
        }

        // 2. Step-level env (overrides pipeline)
        for (k, v) in &step_def.env {
            env_vars.retain(|e| e.name != *k);
            env_vars.push(EnvVar {
                name: k.clone(),
                value: v.clone(),
            });
        }

        // 3. Run-level env_vars (vault secrets + caller env — overrides everything)
        for (k, v) in &run.env_vars {
            env_vars.retain(|e| e.name != *k);
            env_vars.push(EnvVar {
                name: k.clone(),
                value: v.clone(),
            });
        }

        // 4. Built-in pipeline env vars (highest priority)
        let builtins = [
            ("PIPELINE_RUN_ID", run.id.as_str()),
            ("PIPELINE_REF", run.ref_name.as_str()),
            ("PIPELINE_SHA", run.commit_sha.as_str()),
            ("PIPELINE_BRANCH", &branch),
            (
                "PIPELINE_EVENT",
                match &run.trigger {
                    muli_core::pipeline::PipelineTrigger::Push { .. } => "push",
                    muli_core::pipeline::PipelineTrigger::PullRequest { .. } => "pull_request",
                    muli_core::pipeline::PipelineTrigger::Manual { .. } => "manual",
                    muli_core::pipeline::PipelineTrigger::Schedule { .. } => "schedule",
                    muli_core::pipeline::PipelineTrigger::Retry { .. } => "retry",
                },
            ),
            ("PIPELINE_STEP_NAME", &step_def.name),
        ];
        for (k, v) in &builtins {
            env_vars.retain(|e| e.name != *k);
            env_vars.push(EnvVar {
                name: k.to_string(),
                value: v.to_string(),
            });
        }

        if let Some(url) = clone_url {
            env_vars.push(EnvVar {
                name: "PIPELINE_CLONE_URL".to_string(),
                value: url.to_string(),
            });
        }

        // Build commands with auto-checkout prepended
        let mut commands = Vec::new();
        if clone_url.is_some() {
            commands.push(
                "git clone \"$PIPELINE_CLONE_URL\" /workspace && cd /workspace && git checkout \"$PIPELINE_SHA\"".to_string()
            );
        }
        commands.extend(step_def.commands.clone());

        // Parse resource limits
        let (cpu, memory) = match &step_def.resources {
            Some(r) => (
                r.cpu.clone().unwrap_or_else(|| "1000m".to_string()),
                r.memory.clone().unwrap_or_else(|| "512Mi".to_string()),
            ),
            None => ("1000m".to_string(), "512Mi".to_string()),
        };
        let timeout = step_def.timeout.unwrap_or(1800);

        let spec = JobSpec {
            deployment_id: run.id.clone(),
            project_id: run.repo_id.clone(),
            workspace_id: run.tenant_id.clone(),
            tenant_id: run.tenant_id.clone(),
            runner_image: step_def.image.clone(),
            env_vars,
            resources: ResourceSpec {
                cpu_request: cpu.clone(),
                cpu_limit: cpu,
                memory_request: memory.clone(),
                memory_limit: memory,
                timeout_seconds: timeout,
            },
            priority_tier: PriorityTier::Standard,
            framework: "pipeline".to_string(),
            idempotency_key: None, // Matrix-expanded steps need unique keys; let Job::new() generate unique IDs
            registry_credentials: None,
            commands,
        };

        Job::new(spec)
    }

    /// Poll the job store until the job reaches a terminal state or deadline.
    async fn wait_for_job(&self, job_id: &str, deadline: chrono::DateTime<Utc>) -> Option<JobState> {
        loop {
            if Utc::now() > deadline {
                warn!(job_id = %job_id, "pipeline deadline exceeded while waiting for job");
                return Some(JobState::TimedOut);
            }

            match self.job_store.get_job(job_id).await {
                Ok(Some(job)) if job.state.is_terminal() => {
                    return Some(job.state);
                }
                Ok(Some(_)) => {
                    // Still running, wait and poll again
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
                Ok(None) => {
                    error!(job_id = %job_id, "job disappeared from store");
                    return None;
                }
                Err(e) => {
                    error!(job_id = %job_id, error = %e, "error polling job status");
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
    }
}

/// Find the original step definition name for a possibly matrix-expanded step_run name.
/// "test (version=1.80)" → "test", "test" → "test"
fn find_original_name<'a>(sr_name: &str, original_names: &[&'a str]) -> Option<&'a str> {
    for &name in original_names {
        if sr_name == name {
            return Some(name);
        }
        if sr_name.starts_with(name) && sr_name[name.len()..].starts_with(" (") {
            return Some(name);
        }
    }
    None
}

fn trigger_event_str(trigger: &muli_core::pipeline::PipelineTrigger) -> String {
    match trigger {
        muli_core::pipeline::PipelineTrigger::Push { .. } => "push".into(),
        muli_core::pipeline::PipelineTrigger::PullRequest { .. } => "pull_request".into(),
        muli_core::pipeline::PipelineTrigger::Manual { .. } => "manual".into(),
        muli_core::pipeline::PipelineTrigger::Schedule { .. } => "schedule".into(),
        muli_core::pipeline::PipelineTrigger::Retry { .. } => "retry".into(),
    }
}
