// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! DAG executor: processes pipeline steps/jobs level-by-level, submitting each as a Job.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{error, info, warn};

use muli_core::error::Result;
use muli_core::job::model::{
    ArtifactDownload, CheckoutSpec, EnvVar, Job, JobSpec, JobSubstepSpec, PriorityTier,
};
use muli_core::job::state_machine::JobState;
use muli_core::pipeline::{
    FailureStrategy, JobSubstepRun, PipelineRun, PipelineRunState, PipelineTrigger, StepRun,
    StepRunState,
};
use muli_core::resource::limits::ResourceSpec;
use muli_core::traits::{JobStore, PipelineRunStore, StepRunStore};

use crate::dag::graph::DagGraph;
use crate::trigger::matcher::matches_any_path;
use crate::yaml::expression::{ExpressionContext, evaluate_condition};
use crate::yaml::interpolation::interpolate;
use crate::yaml::schema::{JobDef, PipelineDef, ResourceDef, StepDef};

/// Abstraction for submitting jobs to the scheduler.
/// Implemented by the server layer to avoid coupling muli-pipeline to muli-queue.
#[async_trait]
pub trait JobSubmitter: Send + Sync {
    /// Create a Job record and enqueue it for execution. Returns the job ID.
    async fn submit(&self, job: Job) -> Result<String>;
}

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_PIPELINE_DURATION: Duration = Duration::from_secs(3600);

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

    /// Execute a pipeline run end-to-end.
    pub async fn execute(
        &self,
        run: &mut PipelineRun,
        pipeline_def: &PipelineDef,
        step_runs: &[StepRun],
        clone_url: Option<&str>,
    ) -> Result<PipelineRunState> {
        if !pipeline_def.jobs.is_empty() {
            self.execute_jobs(run, pipeline_def, step_runs, clone_url)
                .await
        } else {
            self.execute_steps(run, pipeline_def, step_runs, clone_url)
                .await
        }
    }

    // ── Jobs mode ────────────────────────────────────────────────────────────

    async fn execute_jobs(
        &self,
        run: &mut PipelineRun,
        pipeline_def: &PipelineDef,
        step_runs: &[StepRun],
        clone_url: Option<&str>,
    ) -> Result<PipelineRunState> {
        self.mark_run_started(run).await?;

        let expr_ctx = make_expr_ctx(run);
        let job_names: Vec<String> = pipeline_def.jobs.keys().cloned().collect();
        let original_names: Vec<&str> = job_names.iter().map(String::as_str).collect();

        let (step_run_map, original_to_runs) = build_run_maps(step_runs, &original_names);

        self.evaluate_conditions(&run.tenant_id, step_runs, &original_names, &expr_ctx, |n| {
            pipeline_def
                .jobs
                .get(n)
                .and_then(|d| d.condition.as_deref())
                .map(|s| s.to_string())
        })
        .await?;
        self.evaluate_job_paths(&run.tenant_id, run, step_runs, &original_names, |n| {
            pipeline_def
                .jobs
                .get(n)
                .map(|d| d.paths.clone())
                .unwrap_or_default()
        })
        .await?;

        let deps: Vec<(String, Vec<String>)> = pipeline_def
            .jobs
            .iter()
            .map(|(n, d)| (n.clone(), d.needs.clone()))
            .collect();
        let levels = DagGraph::from_steps(&deps).topological_levels();

        info!(run_id = %run.id, levels = levels.len(), "executing pipeline DAG (jobs mode)");

        let deadline = Utc::now() + chrono::Duration::from_std(MAX_PIPELINE_DURATION).unwrap();

        let build_fn = |orig: &str, sr_name: &str| -> Option<Job> {
            let def = pipeline_def.jobs.get(orig)?;
            let sr = *step_run_map.get(sr_name)?;
            Some(self.build_job_from_jobdef(run, pipeline_def, orig, def, clone_url, sr))
        };

        let (had_failure, had_stop_failure) = self
            .execute_dag_levels(
                &run.tenant_id,
                &levels,
                &original_to_runs,
                &step_run_map,
                deadline,
                &build_fn,
            )
            .await?;

        self.finalize_run(run, had_failure, had_stop_failure).await
    }

    // ── Steps mode (legacy) ──────────────────────────────────────────────────

    async fn execute_steps(
        &self,
        run: &mut PipelineRun,
        pipeline_def: &PipelineDef,
        step_runs: &[StepRun],
        clone_url: Option<&str>,
    ) -> Result<PipelineRunState> {
        self.mark_run_started(run).await?;

        let expr_ctx = make_expr_ctx(run);
        let original_names: Vec<&str> =
            pipeline_def.steps.iter().map(|s| s.name.as_str()).collect();

        let step_def_map: HashMap<&str, &StepDef> = pipeline_def
            .steps
            .iter()
            .map(|s| (s.name.as_str(), s))
            .collect();

        let (step_run_map, original_to_runs) = build_run_maps(step_runs, &original_names);

        self.evaluate_conditions(&run.tenant_id, step_runs, &original_names, &expr_ctx, |n| {
            step_def_map
                .get(n)
                .and_then(|d| d.condition.as_deref())
                .map(|s| s.to_string())
        })
        .await?;

        let deps: Vec<(String, Vec<String>)> = pipeline_def
            .steps
            .iter()
            .map(|s| (s.name.clone(), s.needs.clone()))
            .collect();
        let levels = DagGraph::from_steps(&deps).topological_levels();

        info!(run_id = %run.id, levels = levels.len(), "executing pipeline DAG (steps mode)");

        let deadline = Utc::now() + chrono::Duration::from_std(MAX_PIPELINE_DURATION).unwrap();

        let build_fn = |orig: &str, sr_name: &str| -> Option<Job> {
            let def = *step_def_map.get(orig)?;
            let sr = *step_run_map.get(sr_name)?;
            Some(self.build_job(run, pipeline_def, def, clone_url, sr))
        };

        let (had_failure, had_stop_failure) = self
            .execute_dag_levels(
                &run.tenant_id,
                &levels,
                &original_to_runs,
                &step_run_map,
                deadline,
                &build_fn,
            )
            .await?;

        self.finalize_run(run, had_failure, had_stop_failure).await
    }

    // ── Shared level executor ────────────────────────────────────────────────

    /// Process all DAG levels sequentially; within each level jobs run in parallel.
    /// `build_fn(orig_name, sr_name) -> Option<Job>` constructs the Job for a given run.
    /// Returns `(had_failure, had_stop_failure)`.
    async fn execute_dag_levels(
        &self,
        tenant_id: &str,
        levels: &[Vec<&str>],
        original_to_runs: &HashMap<String, Vec<&str>>,
        step_run_map: &HashMap<&str, &StepRun>,
        deadline: chrono::DateTime<Utc>,
        build_fn: &(impl Fn(&str, &str) -> Option<Job> + Sync),
    ) -> Result<(bool, bool)> {
        let mut had_failure = false;
        let mut had_stop_failure = false;

        for (level_idx, level) in levels.iter().enumerate() {
            if had_stop_failure {
                self.cancel_level_runs(tenant_id, level, original_to_runs, step_run_map)
                    .await?;
                continue;
            }

            info!(level = level_idx, jobs = ?level, "processing DAG level");

            // Collect (orig_name, sr_name) pairs; orig is already known from the map.
            let level_run_pairs: Vec<(&str, &str)> = level
                .iter()
                .flat_map(|n| {
                    let orig: &str = n;
                    original_to_runs
                        .get(*n)
                        .map(|v| v.iter().map(move |&sr| (orig, sr)).collect::<Vec<_>>())
                        .unwrap_or_default()
                })
                .collect();

            let (submitted, submit_failed, submit_stop) = self
                .submit_level(tenant_id, &level_run_pairs, step_run_map, build_fn)
                .await?;

            if submit_failed {
                had_failure = true;
            }
            if submit_stop {
                had_stop_failure = true;
            }

            let (wait_failed, wait_stop) = self
                .wait_level(tenant_id, submitted, step_run_map, deadline)
                .await?;

            if wait_failed {
                had_failure = true;
            }
            if wait_stop {
                had_stop_failure = true;
            }
        }

        Ok((had_failure, had_stop_failure))
    }

    /// Submit all ready runs in a level. Returns (submitted pairs, had_failure, had_stop_failure).
    /// Each element of `level_run_pairs` is `(orig_def_name, expanded_sr_name)`.
    async fn submit_level(
        &self,
        tenant_id: &str,
        level_run_pairs: &[(&str, &str)],
        step_run_map: &HashMap<&str, &StepRun>,
        build_fn: &(impl Fn(&str, &str) -> Option<Job> + Sync),
    ) -> Result<(Vec<(String, String)>, bool, bool)> {
        let mut submitted = Vec::new();
        let mut had_failure = false;
        let mut had_stop = false;

        for &(orig_name, sr_name) in level_run_pairs {
            let sr = match step_run_map.get(sr_name) {
                Some(sr) => sr,
                None => continue,
            };

            // Skip if already in a terminal state (e.g. condition-skipped)
            let current = self.step_store.get_step(tenant_id, &sr.id).await?;
            let state = current.as_ref().map(|s| s.state);
            if matches!(
                state,
                Some(StepRunState::Skipped) | Some(StepRunState::Cancelled)
            ) {
                continue;
            }

            let job = match build_fn(orig_name, sr_name) {
                Some(j) => j,
                None => continue,
            };
            let initial_substeps: Vec<JobSubstepRun> = job
                .spec
                .substeps
                .iter()
                .map(|substep| JobSubstepRun::new(substep.name.clone()))
                .collect();

            self.step_store
                .update_step_state(tenant_id, &sr.id, StepRunState::Running)
                .await?;

            match self.job_submitter.submit(job).await {
                Ok(job_id) => {
                    if let Some(mut step) = self.step_store.get_step(tenant_id, &sr.id).await? {
                        step.job_id = Some(job_id.clone());
                        step.started_at = Some(Utc::now());
                        step.error_message = None;
                        if step.substeps.is_empty() {
                            step.substeps = initial_substeps.clone();
                        }
                        step.updated_at = Utc::now();
                        self.step_store.update_step(&step).await?;
                    }
                    submitted.push((sr_name.to_string(), job_id));
                    info!(job = %sr_name, "job submitted");
                }
                Err(e) => {
                    error!(job = %sr_name, error = %e, "failed to submit job");
                    if let Some(mut step) = self.step_store.get_step(tenant_id, &sr.id).await? {
                        step.state = StepRunState::Failed;
                        step.finished_at = Some(Utc::now());
                        step.error_message = Some(format!("Job submission failed: {e}"));
                        step.updated_at = Utc::now();
                        self.step_store.update_step(&step).await?;
                    } else {
                        self.step_store
                            .update_step_state(tenant_id, &sr.id, StepRunState::Failed)
                            .await?;
                    }
                    had_failure = true;
                    if sr.failure_strategy == FailureStrategy::Stop {
                        had_stop = true;
                    }
                }
            }
        }

        Ok((submitted, had_failure, had_stop))
    }

    /// Wait for all submitted jobs to finish; returns (had_failure, had_stop_failure).
    async fn wait_level(
        &self,
        tenant_id: &str,
        submitted: Vec<(String, String)>,
        step_run_map: &HashMap<&str, &StepRun>,
        deadline: chrono::DateTime<Utc>,
    ) -> Result<(bool, bool)> {
        let mut had_failure = false;
        let mut had_stop = false;

        for (step_name, job_id) in &submitted {
            let sr = match step_run_map.get(step_name.as_str()) {
                Some(sr) => sr,
                None => continue,
            };

            let job_state = self.wait_for_job(job_id, deadline).await;
            let step_state = match job_state {
                Some(JobState::Succeeded) => StepRunState::Succeeded,
                Some(JobState::Cancelled) => StepRunState::Cancelled,
                _ => StepRunState::Failed,
            };
            let error_message = if step_state == StepRunState::Failed {
                self.job_store.get_job(job_id).await?.and_then(|job| {
                    job.result.and_then(|result| {
                        if result.message.trim().is_empty() {
                            None
                        } else {
                            Some(result.message)
                        }
                    })
                })
            } else {
                None
            };

            if let Some(mut step) = self.step_store.get_step(tenant_id, &sr.id).await? {
                step.state = step_state;
                step.finished_at = Some(Utc::now());
                step.error_message = error_message;
                step.updated_at = Utc::now();
                self.step_store.update_step(&step).await?;
            }

            info!(job = %step_name, state = ?step_state, "job completed");

            if step_state == StepRunState::Failed {
                had_failure = true;
                if sr.failure_strategy == FailureStrategy::Stop {
                    had_stop = true;
                }
            }
        }

        Ok((had_failure, had_stop))
    }

    /// Cancel all step_runs belonging to the given level (called on stop-failure).
    async fn cancel_level_runs(
        &self,
        tenant_id: &str,
        level: &[&str],
        original_to_runs: &HashMap<String, Vec<&str>>,
        step_run_map: &HashMap<&str, &StepRun>,
    ) -> Result<()> {
        for &orig_name in level {
            for &rn in original_to_runs
                .get(orig_name)
                .map(Vec::as_slice)
                .unwrap_or(&[])
            {
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
        Ok(())
    }

    /// Evaluate `if` conditions and mark non-matching runs as Skipped.
    async fn evaluate_conditions(
        &self,
        tenant_id: &str,
        step_runs: &[StepRun],
        original_names: &[&str],
        expr_ctx: &ExpressionContext,
        condition_for: impl Fn(&str) -> Option<String>,
    ) -> Result<()> {
        for sr in step_runs {
            if let Some(orig) = find_original_name(&sr.step_name, original_names) {
                if let Some(cond) = condition_for(orig) {
                    if !evaluate_condition(&cond, expr_ctx) {
                        self.step_store
                            .update_step_state(tenant_id, &sr.id, StepRunState::Skipped)
                            .await?;
                        info!(step = %sr.step_name, "skipped (condition not met)");
                    }
                }
            }
        }
        Ok(())
    }

    async fn evaluate_job_paths(
        &self,
        tenant_id: &str,
        run: &PipelineRun,
        step_runs: &[StepRun],
        original_names: &[&str],
        paths_for: impl Fn(&str) -> Vec<String>,
    ) -> Result<()> {
        let Some(changed_paths) = changed_paths_for_trigger(&run.trigger) else {
            return Ok(());
        };

        for sr in step_runs {
            if let Some(orig) = find_original_name(&sr.step_name, original_names) {
                let job_paths = paths_for(orig);
                if !job_paths.is_empty() && !matches_any_path(&job_paths, changed_paths) {
                    self.step_store
                        .update_step_state(tenant_id, &sr.id, StepRunState::Skipped)
                        .await?;
                    info!(step = %sr.step_name, "skipped (changed paths do not match job filter)");
                }
            }
        }

        Ok(())
    }

    async fn mark_run_started(&self, run: &mut PipelineRun) -> Result<()> {
        run.state = PipelineRunState::Running;
        run.started_at = Some(Utc::now());
        run.updated_at = Utc::now();
        self.run_store.update_run(run).await
    }

    async fn finalize_run(
        &self,
        run: &mut PipelineRun,
        had_failure: bool,
        had_stop_failure: bool,
    ) -> Result<PipelineRunState> {
        let state = compute_final_state(had_failure, had_stop_failure);
        run.state = state;
        run.finished_at = Some(Utc::now());
        run.updated_at = Utc::now();
        self.run_store.update_run(run).await?;
        info!(run_id = %run.id, state = ?state, "pipeline run completed");
        Ok(state)
    }

    // ── Job builders ─────────────────────────────────────────────────────────

    fn build_job_from_jobdef(
        &self,
        run: &PipelineRun,
        pipeline_def: &PipelineDef,
        job_name: &str,
        job_def: &JobDef,
        clone_url: Option<&str>,
        step_run: &StepRun,
    ) -> Job {
        let env_vars = build_env_vars(
            &pipeline_def.env,
            &job_def.env,
            run,
            "PIPELINE_JOB_NAME",
            job_name,
            None, // jobs mode: checkout is host-side, no PIPELINE_CLONE_URL
        );

        let checkout = clone_url.map(|url| CheckoutSpec {
            clone_url: url.to_string(),
            sha: run.commit_sha.clone(),
            submodules: pipeline_def.checkout.submodules,
        });

        let artifact_downloads: Vec<ArtifactDownload> = job_def
            .needs
            .iter()
            .filter_map(|needed| {
                let needed_def = pipeline_def.jobs.get(needed)?;
                if needed_def.artifact_upload_paths().is_empty() {
                    return None;
                }
                Some(ArtifactDownload {
                    run_id: run.id.clone(),
                    job_name: needed.clone(),
                })
            })
            .collect();

        let artifact_upload_paths = job_def.artifact_upload_paths();
        let artifact_upload_key = (!artifact_upload_paths.is_empty()).then(|| job_name.to_string());
        let env_map: HashMap<String, String> = env_vars
            .iter()
            .map(|e| (e.name.clone(), e.value.clone()))
            .collect();
        let secret_map: HashMap<String, String> = run.env_vars.clone();
        let substeps: Vec<JobSubstepSpec> = job_def
            .execution_substeps()
            .into_iter()
            .map(|step| JobSubstepSpec {
                name: step.name,
                commands: step
                    .commands
                    .into_iter()
                    .map(|cmd| interpolate(&cmd, &secret_map, &env_map))
                    .collect(),
            })
            .collect();

        let (cpu, memory, timeout) = resolve_resources(job_def.resources.as_ref(), job_def.timeout);
        let runner_image = job_def
            .effective_image(pipeline_def.image.as_deref())
            .unwrap_or("alpine:latest")
            .to_string();

        Job::new(JobSpec {
            deployment_id: run.id.clone(),
            project_id: run.repo_id.clone(),
            workspace_id: run.tenant_id.clone(),
            tenant_id: run.tenant_id.clone(),
            runner_image,
            env_vars,
            resources: make_resource_spec(cpu, memory, timeout),
            priority_tier: PriorityTier::Standard,
            framework: "pipeline".to_string(),
            idempotency_key: None,
            registry_credentials: None,
            commands: Vec::new(),
            substeps,
            checkout,
            artifact_downloads,
            artifact_upload_paths,
            artifact_upload_key,
            pipeline_step_run_id: Some(step_run.id.clone()),
        })
    }

    fn build_job(
        &self,
        run: &PipelineRun,
        pipeline_def: &PipelineDef,
        step_def: &StepDef,
        clone_url: Option<&str>,
        step_run: &StepRun,
    ) -> Job {
        let env_vars = build_env_vars(
            &pipeline_def.env,
            &step_def.env,
            run,
            "PIPELINE_STEP_NAME",
            &step_def.name,
            clone_url,
        );

        let mut commands = Vec::new();
        if clone_url.is_some() {
            commands.push(
                "git clone \"$PIPELINE_CLONE_URL\" /workspace && cd /workspace && git checkout \"$PIPELINE_SHA\"".to_string()
            );
        }
        let env_map: HashMap<String, String> = env_vars
            .iter()
            .map(|e| (e.name.clone(), e.value.clone()))
            .collect();
        let secret_map: HashMap<String, String> = run.env_vars.clone();
        commands.extend(
            step_def
                .commands
                .iter()
                .map(|cmd| interpolate(cmd, &secret_map, &env_map)),
        );

        let (cpu, memory, timeout) =
            resolve_resources(step_def.resources.as_ref(), step_def.timeout);

        Job::new(JobSpec {
            deployment_id: run.id.clone(),
            project_id: run.repo_id.clone(),
            workspace_id: run.tenant_id.clone(),
            tenant_id: run.tenant_id.clone(),
            runner_image: step_def.image.clone(),
            env_vars,
            resources: make_resource_spec(cpu, memory, timeout),
            priority_tier: PriorityTier::Standard,
            framework: "pipeline".to_string(),
            idempotency_key: None,
            registry_credentials: None,
            commands,
            substeps: vec![],
            checkout: None,
            artifact_downloads: vec![],
            artifact_upload_paths: vec![],
            artifact_upload_key: None,
            pipeline_step_run_id: Some(step_run.id.clone()),
        })
    }

    // ── Polling ───────────────────────────────────────────────────────────────

    async fn wait_for_job(
        &self,
        job_id: &str,
        deadline: chrono::DateTime<Utc>,
    ) -> Option<JobState> {
        loop {
            if Utc::now() > deadline {
                warn!(job_id, "pipeline deadline exceeded");
                return Some(JobState::TimedOut);
            }
            match self.job_store.get_job(job_id).await {
                Ok(Some(job)) if job.state.is_terminal() => return Some(job.state),
                Ok(Some(_)) => tokio::time::sleep(POLL_INTERVAL).await,
                Ok(None) => {
                    error!(job_id, "job disappeared from store");
                    return None;
                }
                Err(e) => {
                    error!(job_id, error = %e, "error polling job status");
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
    }
}

// ── Free helpers ──────────────────────────────────────────────────────────────

/// Build the env var list: pipeline → item → run → builtins.
/// `clone_url` is only set in steps mode (adds PIPELINE_CLONE_URL).
fn build_env_vars(
    pipeline_env: &std::collections::HashMap<String, String>,
    item_env: &std::collections::HashMap<String, String>,
    run: &PipelineRun,
    name_key: &str,
    name_val: &str,
    clone_url: Option<&str>,
) -> Vec<EnvVar> {
    let mut env: Vec<EnvVar> = Vec::new();

    let branch = run.ref_name.replace("refs/heads/", "");

    // 1. Pipeline-level
    for (k, v) in pipeline_env {
        env.push(EnvVar {
            name: k.clone(),
            value: v.clone(),
        });
    }
    // 2. Item-level (overrides pipeline)
    for (k, v) in item_env {
        env.retain(|e| e.name != *k);
        env.push(EnvVar {
            name: k.clone(),
            value: v.clone(),
        });
    }
    // 3. Run-level env_vars (vault secrets + caller — overrides everything)
    for (k, v) in &run.env_vars {
        env.retain(|e| e.name != *k);
        env.push(EnvVar {
            name: k.clone(),
            value: v.clone(),
        });
    }
    // 4. Built-in vars (always highest priority)
    let event = trigger_event_str(&run.trigger);
    let builtins: &[(&str, &str)] = &[
        ("PIPELINE_RUN_ID", &run.id),
        ("PIPELINE_REF", &run.ref_name),
        ("PIPELINE_SHA", &run.commit_sha),
        ("PIPELINE_BRANCH", &branch),
        ("PIPELINE_EVENT", &event),
        (name_key, name_val),
    ];
    for (k, v) in builtins {
        env.retain(|e| e.name != *k);
        env.push(EnvVar {
            name: k.to_string(),
            value: v.to_string(),
        });
    }
    if let Some(url) = clone_url {
        env.retain(|e| e.name != "PIPELINE_CLONE_URL");
        env.push(EnvVar {
            name: "PIPELINE_CLONE_URL".into(),
            value: url.to_string(),
        });
    }

    // Interpolate ${{ secrets.X }} and ${{ env.X }} in all env var values.
    // Build lookup maps from the assembled env vars (secrets are in run.env_vars).
    let env_map: HashMap<String, String> = env
        .iter()
        .map(|e| (e.name.clone(), e.value.clone()))
        .collect();
    let secret_map: HashMap<String, String> = run.env_vars.clone();
    for e in &mut env {
        e.value = interpolate(&e.value, &secret_map, &env_map);
    }

    env
}

/// Parse CPU/memory/timeout from an optional ResourceDef, applying defaults.
fn resolve_resources(res: Option<&ResourceDef>, timeout: Option<u64>) -> (String, String, u64) {
    let cpu = res
        .and_then(|r| r.cpu.clone())
        .unwrap_or_else(|| "1000m".into());
    let memory = res
        .and_then(|r| r.memory.clone())
        .unwrap_or_else(|| "512Mi".into());
    (cpu, memory, timeout.unwrap_or(1800))
}

fn make_resource_spec(cpu: String, memory: String, timeout: u64) -> ResourceSpec {
    ResourceSpec {
        cpu_request: cpu.clone(),
        cpu_limit: cpu,
        memory_request: memory.clone(),
        memory_limit: memory,
        timeout_seconds: timeout,
    }
}

fn compute_final_state(had_failure: bool, had_stop_failure: bool) -> PipelineRunState {
    if had_stop_failure {
        PipelineRunState::Failed
    } else if had_failure {
        PipelineRunState::Degraded
    } else {
        PipelineRunState::Succeeded
    }
}

fn make_expr_ctx(run: &PipelineRun) -> ExpressionContext {
    ExpressionContext {
        branch: run.ref_name.replace("refs/heads/", ""),
        event: trigger_event_str(&run.trigger),
        tag: None,
    }
}

/// Build step_run_name → &StepRun and orig_name → [expanded names] maps.
/// The `original_to_runs` map uses owned `String` keys to avoid lifetime coupling
/// between `step_runs` and `original_names`.
fn build_run_maps<'a>(
    step_runs: &'a [StepRun],
    original_names: &[&str],
) -> (HashMap<&'a str, &'a StepRun>, HashMap<String, Vec<&'a str>>) {
    let step_run_map: HashMap<&'a str, &'a StepRun> = step_runs
        .iter()
        .map(|s| (s.step_name.as_str(), s))
        .collect();

    let mut original_to_runs: HashMap<String, Vec<&'a str>> = HashMap::new();
    for sr in step_runs {
        if let Some(orig) = find_original_name(&sr.step_name, original_names) {
            original_to_runs
                .entry(orig.to_string())
                .or_default()
                .push(&sr.step_name);
        }
    }

    (step_run_map, original_to_runs)
}

/// Find the base definition name for a possibly matrix-expanded run name.
/// e.g. `"test (version=1.80)"` → `"test"`.
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

fn trigger_event_str(trigger: &PipelineTrigger) -> String {
    match trigger {
        PipelineTrigger::Push { .. } => "push".into(),
        PipelineTrigger::PullRequest { .. } => "pull_request".into(),
        PipelineTrigger::Manual { .. } => "manual".into(),
        PipelineTrigger::Schedule { .. } => "schedule".into(),
        PipelineTrigger::Retry { .. } => "retry".into(),
    }
}

fn changed_paths_for_trigger(trigger: &PipelineTrigger) -> Option<&[String]> {
    match trigger {
        PipelineTrigger::Push { changed_paths, .. }
        | PipelineTrigger::PullRequest { changed_paths, .. } => Some(changed_paths.as_slice()),
        PipelineTrigger::Manual { .. }
        | PipelineTrigger::Schedule { .. }
        | PipelineTrigger::Retry { .. } => None,
    }
}
