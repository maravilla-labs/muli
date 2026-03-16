// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Directed acyclic graph for pipeline step ordering.

use std::collections::{HashMap, HashSet};

/// A directed acyclic graph of pipeline steps.
pub struct DagGraph<'a> {
    /// step_name -> list of dependency step names
    adj: HashMap<&'a str, Vec<&'a str>>,
    /// All step names in definition order.
    names: Vec<&'a str>,
}

impl<'a> DagGraph<'a> {
    /// Build a DAG from step definitions.
    pub fn from_steps(steps: &'a [(String, Vec<String>)]) -> Self {
        let mut adj = HashMap::new();
        let mut names = Vec::new();
        for (name, deps) in steps {
            adj.insert(name.as_str(), deps.iter().map(|d| d.as_str()).collect());
            names.push(name.as_str());
        }
        Self { adj, names }
    }

    /// Compute topological levels. Each level contains steps that can run in parallel.
    pub fn topological_levels(&self) -> Vec<Vec<&'a str>> {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut reverse: HashMap<&str, Vec<&str>> = HashMap::new();

        for &name in &self.names {
            in_degree.entry(name).or_insert(0);
            if let Some(deps) = self.adj.get(name) {
                *in_degree.entry(name).or_insert(0) += deps.len();
                for &dep in deps {
                    reverse.entry(dep).or_default().push(name);
                }
            }
        }

        let mut levels = Vec::new();
        let mut remaining: HashSet<&str> = self.names.iter().copied().collect();

        loop {
            let level: Vec<&str> = remaining
                .iter()
                .filter(|&&n| *in_degree.get(n).unwrap_or(&0) == 0)
                .copied()
                .collect();

            if level.is_empty() {
                break;
            }

            for &node in &level {
                remaining.remove(node);
                if let Some(dependents) = reverse.get(node) {
                    for &dep in dependents {
                        if let Some(d) = in_degree.get_mut(dep) {
                            *d = d.saturating_sub(1);
                        }
                    }
                }
            }

            levels.push(level);
        }

        levels
    }

    /// Get the direct dependencies of a step.
    pub fn dependencies_of(&self, step_name: &str) -> Vec<&'a str> {
        self.adj.get(step_name).cloned().unwrap_or_default()
    }

    /// Check if the graph has a cycle.
    pub fn has_cycle(&self) -> bool {
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();
        for &name in &self.names {
            if !visited.contains(name) {
                if self.cycle_dfs(name, &mut visited, &mut in_stack) {
                    return true;
                }
            }
        }
        false
    }

    fn cycle_dfs(
        &self,
        node: &'a str,
        visited: &mut HashSet<&'a str>,
        in_stack: &mut HashSet<&'a str>,
    ) -> bool {
        visited.insert(node);
        in_stack.insert(node);
        if let Some(deps) = self.adj.get(node) {
            for &dep in deps {
                if !visited.contains(dep) {
                    if self.cycle_dfs(dep, visited, in_stack) {
                        return true;
                    }
                } else if in_stack.contains(dep) {
                    return true;
                }
            }
        }
        in_stack.remove(node);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_dag() {
        let steps = vec![
            ("a".into(), vec![]),
            ("b".into(), vec!["a".into()]),
            ("c".into(), vec!["b".into()]),
        ];
        let dag = DagGraph::from_steps(&steps);
        let levels = dag.topological_levels();
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], vec!["a"]);
        assert_eq!(levels[1], vec!["b"]);
        assert_eq!(levels[2], vec!["c"]);
    }

    #[test]
    fn test_parallel_dag() {
        let steps = vec![
            ("a".into(), vec![]),
            ("b".into(), vec![]),
            ("c".into(), vec!["a".into(), "b".into()]),
        ];
        let dag = DagGraph::from_steps(&steps);
        let levels = dag.topological_levels();
        assert_eq!(levels.len(), 2);
        assert!(levels[0].contains(&"a"));
        assert!(levels[0].contains(&"b"));
        assert_eq!(levels[1], vec!["c"]);
    }

    #[test]
    fn test_cycle() {
        let steps = vec![
            ("a".into(), vec!["b".into()]),
            ("b".into(), vec!["a".into()]),
        ];
        let dag = DagGraph::from_steps(&steps);
        assert!(dag.has_cycle());
    }
}
