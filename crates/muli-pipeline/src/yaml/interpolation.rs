// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Template interpolation for `${{ secrets.X }}` and `${{ env.X }}` expressions in pipeline YAML.

use std::collections::HashMap;

use tracing::warn;

/// Interpolate `${{ secrets.NAME }}` and `${{ env.NAME }}` references in the input string.
///
/// - `${{ secrets.X }}` is replaced with the value from `secrets`
/// - `${{ env.X }}` is replaced with the value from `env`
/// - Unresolved references are left as-is and a warning is logged.
pub fn interpolate(
    input: &str,
    secrets: &HashMap<String, String>,
    env: &HashMap<String, String>,
) -> String {
    let mut result = String::with_capacity(input.len());
    let mut remaining = input;

    while let Some(start) = remaining.find("${{") {
        result.push_str(&remaining[..start]);
        let after_start = &remaining[start + 3..];

        if let Some(end) = after_start.find("}}") {
            let expr = after_start[..end].trim();
            let replacement = if let Some(name) = expr.strip_prefix("secrets.") {
                let name = name.trim();
                match secrets.get(name) {
                    Some(val) => Some(val.as_str()),
                    None => {
                        warn!(name = %name, "unresolved secret reference: ${{{{ secrets.{name} }}}}");
                        None
                    }
                }
            } else if let Some(name) = expr.strip_prefix("env.") {
                let name = name.trim();
                match env.get(name) {
                    Some(val) => Some(val.as_str()),
                    None => {
                        warn!(name = %name, "unresolved env reference: ${{{{ env.{name} }}}}");
                        None
                    }
                }
            } else {
                None
            };

            match replacement {
                Some(val) => result.push_str(val),
                None => {
                    // Leave unresolved references as-is
                    result.push_str(&remaining[start..start + 3 + end + 2]);
                }
            }
            remaining = &after_start[end + 2..];
        } else {
            // No closing }}, push the rest as-is
            result.push_str(&remaining[start..]);
            remaining = "";
        }
    }

    result.push_str(remaining);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_secrets() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("DB_URL".into(), "postgres://localhost/db".into());
        m.insert("API_KEY".into(), "sk-1234".into());
        m
    }

    fn make_env() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("HOME".into(), "/home/user".into());
        m.insert("CI".into(), "true".into());
        m
    }

    #[test]
    fn secret_interpolation() {
        let secrets = make_secrets();
        let env = HashMap::new();
        let input = "echo ${{ secrets.DB_URL }}";
        let result = interpolate(input, &secrets, &env);
        assert_eq!(result, "echo postgres://localhost/db");
    }

    #[test]
    fn env_interpolation() {
        let secrets = HashMap::new();
        let env = make_env();
        let input = "cd ${{ env.HOME }}";
        let result = interpolate(input, &secrets, &env);
        assert_eq!(result, "cd /home/user");
    }

    #[test]
    fn mixed_interpolation() {
        let secrets = make_secrets();
        let env = make_env();
        let input =
            "curl -H 'Authorization: ${{ secrets.API_KEY }}' http://localhost?ci=${{ env.CI }}";
        let result = interpolate(input, &secrets, &env);
        assert_eq!(
            result,
            "curl -H 'Authorization: sk-1234' http://localhost?ci=true"
        );
    }

    #[test]
    fn unresolved_left_as_is() {
        let secrets = HashMap::new();
        let env = HashMap::new();
        let input = "echo ${{ secrets.MISSING }}";
        let result = interpolate(input, &secrets, &env);
        assert_eq!(result, "echo ${{ secrets.MISSING }}");
    }

    #[test]
    fn no_expressions() {
        let input = "echo hello world";
        let result = interpolate(input, &HashMap::new(), &HashMap::new());
        assert_eq!(result, "echo hello world");
    }

    #[test]
    fn whitespace_in_expression() {
        let secrets = make_secrets();
        let input = "echo ${{   secrets.DB_URL   }}";
        let result = interpolate(input, &secrets, &HashMap::new());
        assert_eq!(result, "echo postgres://localhost/db");
    }

    #[test]
    fn multiple_on_same_line() {
        let secrets = make_secrets();
        let input = "${{ secrets.DB_URL }} and ${{ secrets.API_KEY }}";
        let result = interpolate(input, &secrets, &HashMap::new());
        assert_eq!(result, "postgres://localhost/db and sk-1234");
    }

    #[test]
    fn unclosed_expression() {
        let input = "echo ${{ secrets.DB_URL";
        let result = interpolate(input, &HashMap::new(), &HashMap::new());
        assert_eq!(result, "echo ${{ secrets.DB_URL");
    }

    #[test]
    fn unknown_prefix_left_as_is() {
        let input = "echo ${{ vars.SOMETHING }}";
        let result = interpolate(input, &HashMap::new(), &HashMap::new());
        assert_eq!(result, "echo ${{ vars.SOMETHING }}");
    }

    #[test]
    fn empty_input() {
        let result = interpolate("", &HashMap::new(), &HashMap::new());
        assert_eq!(result, "");
    }
}
