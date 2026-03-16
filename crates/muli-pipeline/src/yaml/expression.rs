// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Simple condition evaluator for step `if` expressions.

/// Context for evaluating step `if` conditions.
pub struct ExpressionContext {
    pub branch: String,
    pub event: String,
    pub tag: Option<String>,
}

const MAX_CONDITION_LENGTH: usize = 512;
const MAX_CONDITION_PARTS: usize = 10;

/// Evaluate a simple condition expression.
/// Supports: `branch == 'main'`, `event == 'push'`, `branch == 'main' && event == 'push'`
pub fn evaluate_condition(expr: &str, ctx: &ExpressionContext) -> bool {
    if expr.len() > MAX_CONDITION_LENGTH {
        return false;
    }
    let parts: Vec<&str> = expr.split("&&").collect();
    if parts.len() > MAX_CONDITION_PARTS {
        return false;
    }
    for part in parts {
        let part = part.trim();
        if !evaluate_single(part, ctx) {
            return false;
        }
    }
    true
}

fn evaluate_single(expr: &str, ctx: &ExpressionContext) -> bool {
    let expr = expr.trim();
    if let Some((lhs, rhs)) = expr.split_once("==") {
        let lhs = lhs.trim();
        let rhs = unquote(rhs.trim());
        match lhs {
            "branch" => ctx.branch == rhs,
            "event" => ctx.event == rhs,
            "tag" => ctx.tag.as_deref() == Some(rhs),
            _ => false,
        }
    } else if let Some((lhs, rhs)) = expr.split_once("!=") {
        let lhs = lhs.trim();
        let rhs = unquote(rhs.trim());
        match lhs {
            "branch" => ctx.branch != rhs,
            "event" => ctx.event != rhs,
            "tag" => ctx.tag.as_deref() != Some(rhs),
            _ => false,
        }
    } else {
        // Unknown expression format — default to true (don't skip)
        true
    }
}

fn unquote(s: &str) -> &str {
    s.trim_matches('\'').trim_matches('"')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(branch: &str, event: &str) -> ExpressionContext {
        ExpressionContext {
            branch: branch.into(),
            event: event.into(),
            tag: None,
        }
    }

    #[test]
    fn test_branch_eq() {
        assert!(evaluate_condition("branch == 'main'", &ctx("main", "push")));
        assert!(!evaluate_condition(
            "branch == 'main'",
            &ctx("develop", "push")
        ));
    }

    #[test]
    fn test_and() {
        assert!(evaluate_condition(
            "branch == 'main' && event == 'push'",
            &ctx("main", "push")
        ));
        assert!(!evaluate_condition(
            "branch == 'main' && event == 'push'",
            &ctx("main", "pull_request")
        ));
    }

    #[test]
    fn test_neq() {
        assert!(evaluate_condition(
            "branch != 'develop'",
            &ctx("main", "push")
        ));
    }

    #[test]
    fn test_empty_condition() {
        // Empty string has no parts to evaluate, so all parts trivially pass
        assert!(evaluate_condition("", &ctx("main", "push")));
    }

    #[test]
    fn test_condition_too_long() {
        let long = "a".repeat(600);
        // Should return false since it exceeds MAX_CONDITION_LENGTH
        assert!(!evaluate_condition(&long, &ctx("main", "push")));
    }

    #[test]
    fn test_too_many_conditions() {
        let cond = (0..15)
            .map(|_| "branch == 'main'")
            .collect::<Vec<_>>()
            .join(" && ");
        // Should return false since it exceeds MAX_CONDITION_PARTS
        assert!(!evaluate_condition(&cond, &ctx("main", "push")));
    }

    #[test]
    fn test_malformed_expression() {
        // "branch >=" is unknown format, should default to true (not crash)
        assert!(evaluate_condition("branch >= 'main'", &ctx("main", "push")));
    }

    #[test]
    fn test_or_not_supported() {
        // || is not supported, but the whole string is split on && only.
        // "branch == 'main' || event == 'push'" is a single && part.
        // It contains ||, which evaluate_single doesn't understand, but
        // it does contain ==, so split_once("==") succeeds:
        // lhs = "branch", rhs = "'main' || event == 'push'"
        // unquoted rhs = "main' || event == 'push"
        // ctx.branch = "main" != "main' || event == 'push" => false
        assert!(!evaluate_condition(
            "branch == 'main' || event == 'push'",
            &ctx("main", "push")
        ));
    }

    #[test]
    fn test_tag_condition() {
        let ctx_with_tag = ExpressionContext {
            branch: "main".into(),
            event: "push".into(),
            tag: Some("v1.0.0".into()),
        };
        assert!(evaluate_condition("tag == 'v1.0.0'", &ctx_with_tag));
        assert!(!evaluate_condition("tag == 'v2.0.0'", &ctx_with_tag));
        // No tag in context
        assert!(!evaluate_condition("tag == 'v1.0.0'", &ctx("main", "push")));
    }

    #[test]
    fn test_multiple_and_conditions() {
        let ctx_main = ExpressionContext {
            branch: "main".into(),
            event: "push".into(),
            tag: Some("v1.0".into()),
        };
        assert!(evaluate_condition(
            "branch == 'main' && event == 'push' && tag == 'v1.0'",
            &ctx_main
        ));
        assert!(!evaluate_condition(
            "branch == 'main' && event == 'push' && tag == 'v2.0'",
            &ctx_main
        ));
    }

    #[test]
    fn test_unknown_variable_is_false() {
        assert!(!evaluate_condition("foo == 'bar'", &ctx("main", "push")));
    }
}
