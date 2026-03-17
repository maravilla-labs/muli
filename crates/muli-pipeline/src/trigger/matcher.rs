// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Match pipeline events against trigger configuration.

use crate::yaml::schema::TriggerDef;

/// An event that may trigger a pipeline.
#[derive(Debug, Clone)]
pub enum PipelineEvent {
    Push {
        branch: String,
        changed_paths: Vec<String>,
    },
    PullRequest {
        target_branch: String,
        event: String,
    },
    Manual,
    Schedule {
        cron: String,
    },
}

/// Check if a pipeline's trigger config matches the given event.
pub fn matches_trigger(trigger: &TriggerDef, event: &PipelineEvent) -> bool {
    match event {
        PipelineEvent::Push {
            branch,
            changed_paths,
        } => {
            if let Some(push) = &trigger.push {
                let branch_match = push.branches.is_empty()
                    || push.branches.iter().any(|b| matches_glob(b, branch));
                let path_match = push.paths.is_empty()
                    || push
                        .paths
                        .iter()
                        .any(|p| changed_paths.iter().any(|cp| matches_glob(p, cp)));
                branch_match && path_match
            } else {
                false
            }
        }
        PipelineEvent::PullRequest {
            target_branch,
            event: ev,
        } => {
            if let Some(pr) = &trigger.pull_request {
                let branch_match = pr.branches.is_empty()
                    || pr.branches.iter().any(|b| matches_glob(b, target_branch));
                let event_match = pr.events.is_empty() || pr.events.iter().any(|e| e == ev);
                branch_match && event_match
            } else {
                false
            }
        }
        PipelineEvent::Manual => trigger.manual,
        PipelineEvent::Schedule { cron } => trigger.schedule.iter().any(|s| s.cron == *cron),
    }
}

/// Simple glob matching: supports `*` as wildcard.
fn matches_glob(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix("**") {
        return value.starts_with(prefix);
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::yaml::schema::{PrTrigger, PushTrigger, ScheduleTrigger, TriggerDef};

    #[test]
    fn test_push_branch_match() {
        let trigger = TriggerDef {
            push: Some(PushTrigger {
                branches: vec!["main".into()],
                paths: vec![],
            }),
            ..Default::default()
        };
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "main".into(),
                changed_paths: vec![]
            }
        ));
        assert!(!matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "develop".into(),
                changed_paths: vec![]
            }
        ));
    }

    #[test]
    fn test_push_path_filter() {
        let trigger = TriggerDef {
            push: Some(PushTrigger {
                branches: vec![],
                paths: vec!["src/**".into()],
            }),
            ..Default::default()
        };
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "main".into(),
                changed_paths: vec!["src/main.rs".into()]
            }
        ));
        assert!(!matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "main".into(),
                changed_paths: vec!["docs/readme.md".into()]
            }
        ));
    }

    #[test]
    fn test_manual() {
        let trigger = TriggerDef {
            manual: true,
            ..Default::default()
        };
        assert!(matches_trigger(&trigger, &PipelineEvent::Manual));
    }

    #[test]
    fn test_pr_trigger() {
        let trigger = TriggerDef {
            pull_request: Some(PrTrigger {
                branches: vec!["main".into()],
                events: vec!["opened".into()],
            }),
            ..Default::default()
        };
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::PullRequest {
                target_branch: "main".into(),
                event: "opened".into()
            }
        ));
        assert!(!matches_trigger(
            &trigger,
            &PipelineEvent::PullRequest {
                target_branch: "main".into(),
                event: "closed".into()
            }
        ));
    }

    #[test]
    fn test_no_triggers_defined() {
        let trigger = TriggerDef::default();
        // No triggers defined: push should not match
        assert!(!matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "main".into(),
                changed_paths: vec![]
            }
        ));
        // Manual should not match (manual defaults to false)
        assert!(!matches_trigger(&trigger, &PipelineEvent::Manual));
    }

    #[test]
    fn test_wildcard_branch() {
        let trigger = TriggerDef {
            push: Some(PushTrigger {
                branches: vec!["feature/*".into()],
                paths: vec![],
            }),
            ..Default::default()
        };
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "feature/add-login".into(),
                changed_paths: vec![]
            }
        ));
        assert!(!matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "main".into(),
                changed_paths: vec![]
            }
        ));
    }

    #[test]
    fn test_empty_push_matches_all() {
        // Empty branches list means match all branches
        let trigger = TriggerDef {
            push: Some(PushTrigger {
                branches: vec![],
                paths: vec![],
            }),
            ..Default::default()
        };
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "any-branch".into(),
                changed_paths: vec![]
            }
        ));
    }

    #[test]
    fn test_schedule_matching() {
        let trigger = TriggerDef {
            schedule: vec![
                ScheduleTrigger {
                    cron: "0 0 * * *".into(),
                },
                ScheduleTrigger {
                    cron: "0 12 * * *".into(),
                },
            ],
            ..Default::default()
        };
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Schedule {
                cron: "0 0 * * *".into()
            }
        ));
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Schedule {
                cron: "0 12 * * *".into()
            }
        ));
        assert!(!matches_trigger(
            &trigger,
            &PipelineEvent::Schedule {
                cron: "0 6 * * *".into()
            }
        ));
    }

    #[test]
    fn test_star_wildcard_matches_all() {
        let trigger = TriggerDef {
            push: Some(PushTrigger {
                branches: vec!["*".into()],
                paths: vec![],
            }),
            ..Default::default()
        };
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "literally-anything".into(),
                changed_paths: vec![]
            }
        ));
    }

    #[test]
    fn test_double_star_path_prefix() {
        let trigger = TriggerDef {
            push: Some(PushTrigger {
                branches: vec![],
                paths: vec!["src/**".into()],
            }),
            ..Default::default()
        };
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "main".into(),
                changed_paths: vec!["src/lib.rs".into()]
            }
        ));
        assert!(matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "main".into(),
                changed_paths: vec!["src/deep/nested/file.rs".into()]
            }
        ));
        assert!(!matches_trigger(
            &trigger,
            &PipelineEvent::Push {
                branch: "main".into(),
                changed_paths: vec!["docs/readme.md".into()]
            }
        ));
    }
}
