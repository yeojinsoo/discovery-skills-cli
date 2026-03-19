use std::collections::{HashMap, HashSet, VecDeque};

use crate::lockfile::{LockedDependency, Lockfile};
use crate::registry::Registry;

/// Information about a missing dependency that needs to be installed.
#[derive(Debug, Clone)]
pub struct MissingDep {
    pub name: String,
    pub ref_version: String,
    pub registry_version: String,
    /// Whether this dependency exists in the registry.
    pub in_registry: bool,
}

/// Information about a version drift between installed and referenced version.
#[derive(Debug, Clone)]
pub struct VersionDrift {
    pub dep_name: String,
    pub installed_version: String,
    pub ref_version: String,
}

/// Information about an installed skill that depends on a given skill.
#[derive(Debug, Clone)]
pub struct DependentSkill {
    pub name: String,
    pub version: String,
}

// ---------------------------------------------------------------------------
// Install helpers
// ---------------------------------------------------------------------------

/// Resolve all transitive missing dependencies for installing a skill.
///
/// Uses DFS post-order traversal to produce a correct topological install order
/// (deepest deps first). This works correctly for all DAG shapes including diamonds.
/// Already-installed dependencies are excluded from the result.
/// If a dependency is not in the registry, it is still included with `in_registry = false`.
pub fn resolve_install_deps(
    skill_name: &str,
    registry: &Registry,
    lockfile: &Lockfile,
) -> Vec<MissingDep> {
    let mut order: Vec<String> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();

    // Collect all transitive deps via DFS post-order (correct topological sort)
    visited.insert(skill_name.to_string());
    if let Some(entry) = registry.skills.get(skill_name) {
        for dep in &entry.depends_on {
            dfs_post_order(&dep.name, registry, &mut visited, &mut order);
        }
    }

    // Build a lookup of which skill references each dep (for ref_version)
    let ref_versions = build_ref_version_map(skill_name, &order, registry);

    // Convert to MissingDep, filtering out already-installed
    order
        .into_iter()
        .filter(|name| lockfile.get_skill(name).is_none())
        .map(|name| {
            let ref_version = ref_versions.get(&name).cloned().unwrap_or_default();
            let (registry_version, in_registry) = match registry.skills.get(&name) {
                Some(e) => (e.version.clone(), true),
                None => (ref_version.clone(), false),
            };
            MissingDep {
                name,
                ref_version,
                registry_version,
                in_registry,
            }
        })
        .collect()
}

/// DFS post-order: visit all children first, then append self.
/// This ensures that for any edge A→B, B appears before A in the output.
fn dfs_post_order(
    name: &str,
    registry: &Registry,
    visited: &mut HashSet<String>,
    order: &mut Vec<String>,
) {
    if visited.contains(name) {
        return; // already processed or circular — safe to skip
    }
    visited.insert(name.to_string());

    // Recurse into this node's dependencies first
    if let Some(entry) = registry.skills.get(name) {
        for dep in &entry.depends_on {
            dfs_post_order(&dep.name, registry, visited, order);
        }
    }

    // Post-order: append after all children
    order.push(name.to_string());
}

/// Build a map from dep name to the ref_version declared by its referrer.
fn build_ref_version_map(
    root: &str,
    deps: &[String],
    registry: &Registry,
) -> HashMap<String, String> {
    let mut map = HashMap::new();
    // Collect all skills in scope (root + all deps)
    let scope: Vec<&str> = std::iter::once(root)
        .chain(deps.iter().map(|s| s.as_str()))
        .collect();

    for &skill_name in &scope {
        if let Some(entry) = registry.skills.get(skill_name) {
            for dep in &entry.depends_on {
                map.entry(dep.name.clone())
                    .or_insert_with(|| dep.ref_version.clone());
            }
        }
    }
    map
}

/// Check version drift for installed dependencies.
///
/// Returns a list of dependencies where the installed version differs
/// from the ref_version declared by the skill.
pub fn check_version_drift(
    skill_name: &str,
    registry: &Registry,
    lockfile: &Lockfile,
) -> Vec<VersionDrift> {
    let entry = match registry.skills.get(skill_name) {
        Some(e) => e,
        None => return vec![],
    };

    entry
        .depends_on
        .iter()
        .filter_map(|dep| {
            let installed = lockfile.get_skill(&dep.name)?;
            if installed.version != dep.ref_version {
                Some(VersionDrift {
                    dep_name: dep.name.clone(),
                    installed_version: installed.version.clone(),
                    ref_version: dep.ref_version.clone(),
                })
            } else {
                None
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Uninstall helpers
// ---------------------------------------------------------------------------

/// Find all installed skills that transitively depend on the given skill (BFS).
///
/// Returns dependents in removal order (leaf dependents first, direct dependents last).
pub fn find_dependents(
    skill_name: &str,
    registry: &Registry,
    lockfile: &Lockfile,
) -> Vec<DependentSkill> {
    let mut result: Vec<DependentSkill> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();

    visited.insert(skill_name.to_string());
    queue.push_back(skill_name.to_string());

    while let Some(current) = queue.pop_front() {
        // Find all installed skills that directly depend on `current`
        for (name, entry) in &registry.skills {
            if visited.contains(name) {
                continue;
            }
            let depends = entry.depends_on.iter().any(|d| d.name == current);
            if !depends {
                continue;
            }
            if let Some(installed) = lockfile.get_skill(name) {
                visited.insert(name.clone());
                queue.push_back(name.clone());
                result.push(DependentSkill {
                    name: name.clone(),
                    version: installed.version.clone(),
                });
            }
        }
    }

    result
}

/// Find all installed skills that transitively depend on the given skill,
/// using only lockfile data (offline mode, no registry needed).
///
/// Returns dependents in removal order.
pub fn find_dependents_offline(
    skill_name: &str,
    lockfile: &Lockfile,
) -> Vec<DependentSkill> {
    let mut result: Vec<DependentSkill> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();

    visited.insert(skill_name.to_string());
    queue.push_back(skill_name.to_string());

    while let Some(current) = queue.pop_front() {
        for (name, installed) in &lockfile.skills {
            if visited.contains(name) {
                continue;
            }
            let depends = installed.depends_on.iter().any(|d| d.name == current);
            if depends {
                visited.insert(name.clone());
                queue.push_back(name.clone());
                result.push(DependentSkill {
                    name: name.clone(),
                    version: installed.version.clone(),
                });
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Update helpers
// ---------------------------------------------------------------------------

/// Find the ref_version that a dependent skill expects for a given dependency.
pub fn get_ref_version(
    dependent_name: &str,
    dep_name: &str,
    registry: &Registry,
) -> Option<String> {
    registry
        .skills
        .get(dependent_name)?
        .depends_on
        .iter()
        .find(|d| d.name == dep_name)
        .map(|d| d.ref_version.clone())
}

// ---------------------------------------------------------------------------
// Conversion helper
// ---------------------------------------------------------------------------

/// Convert registry dependency entries to lockfile dependency format.
pub fn to_locked_deps(registry: &Registry, skill_name: &str) -> Vec<LockedDependency> {
    registry
        .skills
        .get(skill_name)
        .map(|entry| {
            entry
                .depends_on
                .iter()
                .map(|d| LockedDependency {
                    name: d.name.clone(),
                    ref_version: d.ref_version.clone(),
                })
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::parse_registry;

    const SAMPLE_TOML: &str = r#"
[metadata]
repo = "yeojinsoo/discovery-skills-registry"

[skills.base-skill]
version = "1.0.0"
description = "Base skill"

[skills.mid-skill]
version = "1.0.0"
description = "Depends on base-skill"
depends_on = [{ name = "base-skill", ref_version = "1.0.0" }]

[skills.top-skill]
version = "2.0.0"
description = "Depends on mid-skill"
depends_on = [{ name = "mid-skill", ref_version = "1.0.0" }]

[skills.independent-skill]
version = "1.0.0"
description = "No dependencies"
"#;

    const CIRCULAR_TOML: &str = r#"
[metadata]
repo = "yeojinsoo/discovery-skills-registry"

[skills.skill-a]
version = "1.0.0"
description = "Depends on skill-b"
depends_on = [{ name = "skill-b", ref_version = "1.0.0" }]

[skills.skill-b]
version = "1.0.0"
description = "Depends on skill-a"
depends_on = [{ name = "skill-a", ref_version = "1.0.0" }]
"#;

    const MISSING_DEP_TOML: &str = r#"
[metadata]
repo = "yeojinsoo/discovery-skills-registry"

[skills.orphan-skill]
version = "1.0.0"
description = "Depends on a skill not in registry"
depends_on = [{ name = "ghost-skill", ref_version = "0.5.0" }]
"#;

    /// Diamond with cross-edge: E depends on B and C, B depends on A, C depends on A and B.
    /// Correct install order must have A before B, and B before C.
    const DIAMOND_TOML: &str = r#"
[metadata]
repo = "yeojinsoo/discovery-skills-registry"

[skills.skill-a]
version = "1.0.0"
description = "Leaf"

[skills.skill-b]
version = "1.0.0"
description = "Depends on A"
depends_on = [{ name = "skill-a", ref_version = "1.0.0" }]

[skills.skill-c]
version = "1.0.0"
description = "Depends on A and B"
depends_on = [
  { name = "skill-a", ref_version = "1.0.0" },
  { name = "skill-b", ref_version = "1.0.0" },
]

[skills.skill-e]
version = "1.0.0"
description = "Depends on B and C"
depends_on = [
  { name = "skill-b", ref_version = "1.0.0" },
  { name = "skill-c", ref_version = "1.0.0" },
]
"#;

    fn make_lockfile_with(skills: &[(&str, &str)]) -> Lockfile {
        let mut lf = Lockfile::default();
        for (name, version) in skills {
            lf.add_skill(name, version, vec![]);
        }
        lf
    }

    fn make_lockfile_with_deps(
        skills: &[(&str, &str, Vec<LockedDependency>)],
    ) -> Lockfile {
        let mut lf = Lockfile::default();
        for (name, version, deps) in skills {
            lf.add_skill(name, version, deps.clone());
        }
        lf
    }

    // -----------------------------------------------------------------------
    // resolve_install_deps
    // -----------------------------------------------------------------------

    #[test]
    fn test_resolve_install_deps_missing() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = Lockfile::default();

        let missing = resolve_install_deps("mid-skill", &reg, &lockfile);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].name, "base-skill");
        assert!(missing[0].in_registry);
    }

    #[test]
    fn test_resolve_install_deps_already_installed() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = make_lockfile_with(&[("base-skill", "1.0.0")]);

        let missing = resolve_install_deps("mid-skill", &reg, &lockfile);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_resolve_install_deps_no_deps() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = Lockfile::default();

        let missing = resolve_install_deps("independent-skill", &reg, &lockfile);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_resolve_install_deps_transitive() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = Lockfile::default();

        // top-skill -> mid-skill -> base-skill
        let missing = resolve_install_deps("top-skill", &reg, &lockfile);
        assert_eq!(missing.len(), 2);
        // Install order: base-skill first, then mid-skill
        assert_eq!(missing[0].name, "base-skill");
        assert_eq!(missing[1].name, "mid-skill");
    }

    #[test]
    fn test_resolve_install_deps_transitive_partial() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        // base-skill already installed, only mid-skill is missing
        let lockfile = make_lockfile_with(&[("base-skill", "1.0.0")]);

        let missing = resolve_install_deps("top-skill", &reg, &lockfile);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].name, "mid-skill");
    }

    #[test]
    fn test_resolve_install_deps_circular() {
        let reg = parse_registry(CIRCULAR_TOML).unwrap();
        let lockfile = Lockfile::default();

        // skill-a -> skill-b -> skill-a (circular)
        let missing = resolve_install_deps("skill-a", &reg, &lockfile);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].name, "skill-b");
    }

    #[test]
    fn test_resolve_install_deps_not_in_registry() {
        let reg = parse_registry(MISSING_DEP_TOML).unwrap();
        let lockfile = Lockfile::default();

        let missing = resolve_install_deps("orphan-skill", &reg, &lockfile);
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].name, "ghost-skill");
        assert!(!missing[0].in_registry);
        assert_eq!(missing[0].ref_version, "0.5.0");
        assert_eq!(missing[0].registry_version, "0.5.0"); // fallback
    }

    // -----------------------------------------------------------------------
    // check_version_drift
    // -----------------------------------------------------------------------

    #[test]
    fn test_check_version_drift_match() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = make_lockfile_with(&[("base-skill", "1.0.0")]);

        let drifts = check_version_drift("mid-skill", &reg, &lockfile);
        assert!(drifts.is_empty());
    }

    #[test]
    fn test_check_version_drift_mismatch() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = make_lockfile_with(&[("base-skill", "2.0.0")]);

        let drifts = check_version_drift("mid-skill", &reg, &lockfile);
        assert_eq!(drifts.len(), 1);
        assert_eq!(drifts[0].dep_name, "base-skill");
        assert_eq!(drifts[0].installed_version, "2.0.0");
        assert_eq!(drifts[0].ref_version, "1.0.0");
    }

    #[test]
    fn test_check_version_drift_dep_not_installed() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = Lockfile::default(); // base-skill not installed

        let drifts = check_version_drift("mid-skill", &reg, &lockfile);
        assert!(drifts.is_empty()); // no crash, just empty
    }

    // -----------------------------------------------------------------------
    // find_dependents (online)
    // -----------------------------------------------------------------------

    #[test]
    fn test_find_dependents_direct() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = make_lockfile_with(&[
            ("base-skill", "1.0.0"),
            ("mid-skill", "1.0.0"),
        ]);

        let dependents = find_dependents("base-skill", &reg, &lockfile);
        assert_eq!(dependents.len(), 1);
        assert_eq!(dependents[0].name, "mid-skill");
    }

    #[test]
    fn test_find_dependents_transitive() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = make_lockfile_with(&[
            ("base-skill", "1.0.0"),
            ("mid-skill", "1.0.0"),
            ("top-skill", "2.0.0"),
        ]);

        // Removing base-skill should find mid-skill AND top-skill
        let dependents = find_dependents("base-skill", &reg, &lockfile);
        assert_eq!(dependents.len(), 2);
        let names: HashSet<String> = dependents.iter().map(|d| d.name.clone()).collect();
        assert!(names.contains("mid-skill"));
        assert!(names.contains("top-skill"));
    }

    #[test]
    fn test_find_dependents_not_installed() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = make_lockfile_with(&[("base-skill", "1.0.0")]);

        let dependents = find_dependents("base-skill", &reg, &lockfile);
        assert!(dependents.is_empty());
    }

    #[test]
    fn test_find_dependents_none() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();
        let lockfile = make_lockfile_with(&[("independent-skill", "1.0.0")]);

        let dependents = find_dependents("independent-skill", &reg, &lockfile);
        assert!(dependents.is_empty());
    }

    #[test]
    fn test_find_dependents_circular() {
        let reg = parse_registry(CIRCULAR_TOML).unwrap();
        let lockfile = make_lockfile_with(&[
            ("skill-a", "1.0.0"),
            ("skill-b", "1.0.0"),
        ]);

        // Removing skill-a should find skill-b (no infinite loop)
        let dependents = find_dependents("skill-a", &reg, &lockfile);
        assert_eq!(dependents.len(), 1);
        assert_eq!(dependents[0].name, "skill-b");
    }

    // -----------------------------------------------------------------------
    // find_dependents_offline
    // -----------------------------------------------------------------------

    #[test]
    fn test_find_dependents_offline_basic() {
        let lockfile = make_lockfile_with_deps(&[
            ("base-skill", "1.0.0", vec![]),
            (
                "mid-skill",
                "1.0.0",
                vec![LockedDependency {
                    name: "base-skill".to_string(),
                    ref_version: "1.0.0".to_string(),
                }],
            ),
        ]);

        let dependents = find_dependents_offline("base-skill", &lockfile);
        assert_eq!(dependents.len(), 1);
        assert_eq!(dependents[0].name, "mid-skill");
    }

    #[test]
    fn test_find_dependents_offline_transitive() {
        let lockfile = make_lockfile_with_deps(&[
            ("base-skill", "1.0.0", vec![]),
            (
                "mid-skill",
                "1.0.0",
                vec![LockedDependency {
                    name: "base-skill".to_string(),
                    ref_version: "1.0.0".to_string(),
                }],
            ),
            (
                "top-skill",
                "2.0.0",
                vec![LockedDependency {
                    name: "mid-skill".to_string(),
                    ref_version: "1.0.0".to_string(),
                }],
            ),
        ]);

        let dependents = find_dependents_offline("base-skill", &lockfile);
        assert_eq!(dependents.len(), 2);
        let names: HashSet<String> = dependents.iter().map(|d| d.name.clone()).collect();
        assert!(names.contains("mid-skill"));
        assert!(names.contains("top-skill"));
    }

    // -----------------------------------------------------------------------
    // get_ref_version
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_ref_version() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();

        let rv = get_ref_version("mid-skill", "base-skill", &reg);
        assert_eq!(rv, Some("1.0.0".to_string()));

        let rv = get_ref_version("mid-skill", "nonexistent", &reg);
        assert_eq!(rv, None);
    }

    // -----------------------------------------------------------------------
    // to_locked_deps
    // -----------------------------------------------------------------------

    #[test]
    fn test_to_locked_deps() {
        let reg = parse_registry(SAMPLE_TOML).unwrap();

        let deps = to_locked_deps(&reg, "mid-skill");
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].name, "base-skill");
        assert_eq!(deps[0].ref_version, "1.0.0");

        let deps = to_locked_deps(&reg, "independent-skill");
        assert!(deps.is_empty());

        let deps = to_locked_deps(&reg, "nonexistent");
        assert!(deps.is_empty());
    }

    // -----------------------------------------------------------------------
    // Diamond graph topological ordering
    // -----------------------------------------------------------------------

    #[test]
    fn test_resolve_install_deps_diamond_cross_edge() {
        let reg = parse_registry(DIAMOND_TOML).unwrap();
        let lockfile = Lockfile::default();

        // skill-e -> skill-b, skill-c
        // skill-b -> skill-a
        // skill-c -> skill-a, skill-b
        let deps = resolve_install_deps("skill-e", &reg, &lockfile);
        let names: Vec<&str> = deps.iter().map(|d| d.name.as_str()).collect();

        assert_eq!(names.len(), 3);
        // A must come before B (B depends on A)
        let pos_a = names.iter().position(|n| *n == "skill-a").unwrap();
        let pos_b = names.iter().position(|n| *n == "skill-b").unwrap();
        let pos_c = names.iter().position(|n| *n == "skill-c").unwrap();
        assert!(pos_a < pos_b, "A must be installed before B");
        assert!(pos_a < pos_c, "A must be installed before C");
        assert!(pos_b < pos_c, "B must be installed before C");
    }
}
