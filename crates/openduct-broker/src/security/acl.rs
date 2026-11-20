// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Who may do what, in the shape of Kafka's access rules: a rule allows or denies a principal (from an address, or
//! any) one or more operations on topics, groups, the cluster or transactional ids - named exactly, by prefix (a
//! tenant's name space), or all. Nothing is allowed unless a rule allows it, a deny wins over any allow, and super
//! users (named in the rules file, none by default) may do everything. Reading, writing, deleting or altering a
//! resource implies seeing it (Describe). The rules file is re-read when it changes.

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use super::Principal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Resource {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    /// A subject of the schema registry: Read (its versions and schemas), Write (register), Delete,
    /// DescribeConfigs and AlterConfigs (its compatibility level).
    Subject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Pattern {
    /// The name exactly; `*` is every name.
    #[default]
    Literal,
    /// Every name starting with it.
    Prefixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Allow,
    Deny,
}

/// One rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    /// `User:<name>`, or `User:*` for every authenticated principal.
    pub principal: String,
    /// An address, or `*` (the default).
    #[serde(default = "any")]
    pub host: String,
    pub resource: Resource,
    #[serde(default)]
    pub pattern: Pattern,
    /// The resource's name (or prefix); `kafka-cluster` for the cluster.
    pub name: String,
    pub operations: Vec<Operation>,
    pub permission: Permission,
}

fn any() -> String {
    "*".to_owned()
}

/// The rules file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rules {
    /// Principals (`User:<name>`) that may do everything.
    #[serde(default)]
    pub super_users: Vec<String>,
    #[serde(default)]
    pub rules: Vec<Rule>,
    /// Each principal's share of the broker (`User:*` for every one without its own).
    #[serde(default)]
    pub quotas: std::collections::BTreeMap<String, super::quota::Quota>,
}

impl Rules {
    pub fn check(&self) -> Result<(), String> {
        for r in &self.rules {
            if !r.principal.starts_with("User:") || r.principal.len() <= 5 {
                return Err(format!(
                    "a rule's principal must be User:<name> or User:*, not {:?}",
                    r.principal
                ));
            }
            if r.operations.is_empty() {
                return Err(format!("a rule for {} has no operations", r.principal));
            }
            if r.host != "*" && r.host.parse::<IpAddr>().is_err() {
                return Err(format!("a rule's host must be an address or *, not {:?}", r.host));
            }
            if r.name.is_empty() {
                return Err(format!("a rule for {} has no resource name", r.principal));
            }
        }
        for p in self.quotas.keys() {
            if !p.starts_with("User:") || p.len() <= 5 {
                return Err(format!(
                    "a quota's principal must be User:<name> or User:*, not {p:?}"
                ));
            }
        }
        for s in &self.super_users {
            if !s.starts_with("User:") || s == "User:*" {
                return Err(format!("a super user must be one User:<name>, not {s:?}"));
            }
        }
        Ok(())
    }
}

/// The rules, indexed by principal, as used for every request.
#[derive(Debug, Default)]
struct Index {
    super_users: Vec<String>,
    by_principal: HashMap<String, Vec<Rule>>,
    quotas: std::collections::BTreeMap<String, super::quota::Quota>,
}

impl Index {
    fn new(rules: Rules) -> Self {
        let mut by_principal: HashMap<String, Vec<Rule>> = HashMap::new();
        for r in rules.rules {
            by_principal.entry(r.principal.clone()).or_default().push(r);
        }
        Self {
            super_users: rules.super_users,
            by_principal,
            quotas: rules.quotas,
        }
    }

    fn rules_for<'a>(&'a self, principal: &str) -> impl Iterator<Item = &'a Rule> {
        let exact = self.by_principal.get(principal).into_iter().flatten();
        let all = self.by_principal.get("User:*").into_iter().flatten();
        exact.chain(all)
    }
}

fn implied_by(op: Operation) -> &'static [Operation] {
    match op {
        Operation::Describe => &[
            Operation::Describe,
            Operation::Read,
            Operation::Write,
            Operation::Delete,
            Operation::Alter,
        ],
        Operation::DescribeConfigs => &[Operation::DescribeConfigs, Operation::AlterConfigs],
        Operation::Read => &[Operation::Read],
        Operation::Write => &[Operation::Write],
        Operation::Create => &[Operation::Create],
        Operation::Delete => &[Operation::Delete],
        Operation::Alter => &[Operation::Alter],
        Operation::ClusterAction => &[Operation::ClusterAction],
        Operation::AlterConfigs => &[Operation::AlterConfigs],
        Operation::IdempotentWrite => &[Operation::IdempotentWrite],
        Operation::All => &[],
    }
}

fn names_match(rule: &Rule, name: &str) -> bool {
    match rule.pattern {
        Pattern::Literal => rule.name == "*" || rule.name == name,
        Pattern::Prefixed => name.starts_with(&rule.name),
    }
}

/// Decides every request; the rules come from a file re-read when it changes, or are fixed (tests, embedding).
#[derive(Debug)]
pub struct Authorizer {
    file: Option<PathBuf>,
    index: RwLock<Arc<Index>>,
    checked: Mutex<(Instant, Option<SystemTime>)>,
}

const RECHECK: Duration = Duration::from_secs(2);

impl Authorizer {
    pub fn fixed(rules: Rules) -> Result<Self, String> {
        rules.check()?;
        Ok(Self {
            file: None,
            index: RwLock::new(Arc::new(Index::new(rules))),
            checked: Mutex::new((Instant::now(), None)),
        })
    }

    /// Rules from a JSON file (`{"super_users": [...], "rules": [...]}`), re-read when it changes. A changed file
    /// that does not parse is ignored (the rules before it stay) and logged.
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let (rules, modified) = read_rules(path)?;
        Ok(Self {
            file: Some(path.to_owned()),
            index: RwLock::new(Arc::new(Index::new(rules))),
            checked: Mutex::new((Instant::now(), Some(modified))),
        })
    }

    fn current(&self) -> Arc<Index> {
        if let Some(path) = &self.file {
            let mut checked = self.checked.lock();
            if checked.0.elapsed() >= RECHECK {
                checked.0 = Instant::now();
                let modified = std::fs::metadata(path).and_then(|m| m.modified()).ok();
                if modified.is_some() && modified != checked.1 {
                    match read_rules(path) {
                        Ok((rules, m)) => {
                            *self.index.write() = Arc::new(Index::new(rules));
                            checked.1 = Some(m);
                            tracing::info!(file = %path.display(), "access rules re-read");
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "the changed access rules are refused; the rules before stay")
                        }
                    }
                }
            }
        }
        Arc::clone(&self.index.read())
    }

    /// Whether `principal`, connected from `host`, may do `op` on the resource.
    pub fn allows(
        &self,
        principal: &Principal,
        host: IpAddr,
        resource: Resource,
        name: &str,
        op: Operation,
    ) -> bool {
        let index = self.current();
        let who = principal.as_str();
        if index.super_users.iter().any(|s| s == who) {
            return true;
        }
        let host = host.to_string();
        let mut allowed = false;
        for r in index.rules_for(who) {
            if r.resource != resource || !(r.host == "*" || r.host == host) || !names_match(r, name) {
                continue;
            }
            match r.permission {
                Permission::Deny if r.operations.iter().any(|o| *o == op || *o == Operation::All) => {
                    return false;
                }
                Permission::Allow
                    if r.operations
                        .iter()
                        .any(|o| *o == Operation::All || implied_by(op).contains(o)) =>
                {
                    allowed = true;
                }
                _ => {}
            }
        }
        allowed
    }

    /// The principal's share: its own, else every principal's (`User:*`), else none.
    pub fn quota(&self, principal: &Principal) -> super::quota::Quota {
        let index = self.current();
        index
            .quotas
            .get(principal.as_str())
            .or_else(|| index.quotas.get("User:*"))
            .copied()
            .unwrap_or_default()
    }

    /// Whether `principal` may do `op` on at least one resource of the kind (an idempotent producer needs Write on
    /// some topic). A deny on every name (`*`, or an empty prefix) counts.
    pub fn allows_any(&self, principal: &Principal, host: IpAddr, resource: Resource, op: Operation) -> bool {
        let index = self.current();
        let who = principal.as_str();
        if index.super_users.iter().any(|s| s == who) {
            return true;
        }
        let host = host.to_string();
        let applies = |r: &&Rule| r.resource == resource && (r.host == "*" || r.host == host);
        let mut rules = index.rules_for(who).filter(applies);
        let denied_everywhere = index.rules_for(who).filter(applies).any(|r| {
            r.permission == Permission::Deny
                && r.operations.iter().any(|o| *o == op || *o == Operation::All)
                && ((r.pattern == Pattern::Literal && r.name == "*")
                    || (r.pattern == Pattern::Prefixed && r.name.is_empty()))
        });
        !denied_everywhere
            && rules.any(|r| {
                r.permission == Permission::Allow
                    && r.operations.iter().any(|o| *o == op || *o == Operation::All)
            })
    }
}

fn read_rules(path: &Path) -> Result<(Rules, SystemTime), String> {
    let raw = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let modified = std::fs::metadata(path)
        .and_then(|m| m.modified())
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let rules: Rules = serde_json::from_slice(&raw).map_err(|e| format!("{}: {e}", path.display()))?;
    rules.check().map_err(|e| format!("{}: {e}", path.display()))?;
    Ok((rules, modified))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(
        principal: &str,
        resource: Resource,
        pattern: Pattern,
        name: &str,
        ops: &[Operation],
        permission: Permission,
    ) -> Rule {
        Rule {
            principal: principal.to_owned(),
            host: "*".to_owned(),
            resource,
            pattern,
            name: name.to_owned(),
            operations: ops.to_vec(),
            permission,
        }
    }

    fn host() -> IpAddr {
        "10.0.0.5".parse().unwrap()
    }

    #[test]
    fn nothing_is_allowed_without_a_rule_and_a_deny_wins() {
        let a = Authorizer::fixed(Rules {
            quotas: Default::default(),
            super_users: vec!["User:admin".to_owned()],
            rules: vec![
                rule(
                    "User:t1",
                    Resource::Topic,
                    Pattern::Prefixed,
                    "t1.",
                    &[Operation::Read, Operation::Write],
                    Permission::Allow,
                ),
                rule(
                    "User:t1",
                    Resource::Topic,
                    Pattern::Literal,
                    "t1.secret",
                    &[Operation::All],
                    Permission::Deny,
                ),
                rule(
                    "User:*",
                    Resource::Topic,
                    Pattern::Literal,
                    "public",
                    &[Operation::Read],
                    Permission::Allow,
                ),
            ],
        })
        .unwrap();
        let t1 = Principal::user("t1");
        let t2 = Principal::user("t2");
        assert!(a.allows(&t1, host(), Resource::Topic, "t1.orders", Operation::Write));
        assert!(
            a.allows(&t1, host(), Resource::Topic, "t1.orders", Operation::Describe),
            "write implies describe"
        );
        assert!(!a.allows(&t1, host(), Resource::Topic, "t1.orders", Operation::Delete));
        assert!(
            !a.allows(&t1, host(), Resource::Topic, "t1.secret", Operation::Read),
            "deny wins"
        );
        assert!(
            !a.allows(&t1, host(), Resource::Topic, "t2.orders", Operation::Read),
            "another tenant's"
        );
        assert!(!a.allows(&t2, host(), Resource::Topic, "t1.orders", Operation::Read));
        assert!(a.allows(&t2, host(), Resource::Topic, "public", Operation::Read));
        assert!(!a.allows(&t2, host(), Resource::Topic, "public", Operation::Write));
        assert!(!a.allows(&t1, host(), Resource::Cluster, "kafka-cluster", Operation::Alter));
        assert!(a.allows(
            &Principal::user("admin"),
            host(),
            Resource::Cluster,
            "kafka-cluster",
            Operation::Alter
        ));
        assert!(a.allows_any(&t1, host(), Resource::Topic, Operation::Write));
        assert!(!a.allows_any(&t2, host(), Resource::Topic, Operation::Write));
    }

    #[test]
    fn a_rule_may_hold_to_one_address() {
        let mut r = rule(
            "User:svc",
            Resource::Group,
            Pattern::Literal,
            "g",
            &[Operation::Read],
            Permission::Allow,
        );
        r.host = "10.0.0.5".to_owned();
        let a = Authorizer::fixed(Rules {
            quotas: Default::default(),
            super_users: vec![],
            rules: vec![r],
        })
        .unwrap();
        let svc = Principal::user("svc");
        assert!(a.allows(&svc, host(), Resource::Group, "g", Operation::Read));
        assert!(!a.allows(
            &svc,
            "10.0.0.6".parse().unwrap(),
            Resource::Group,
            "g",
            Operation::Read
        ));
    }

    #[test]
    fn bad_rules_are_refused_and_a_changed_file_is_re_read() {
        assert!(
            Rules {
                quotas: Default::default(),
                super_users: vec!["User:*".to_owned()],
                rules: vec![]
            }
            .check()
            .is_err()
        );
        assert!(
            Rules {
                quotas: Default::default(),
                super_users: vec![],
                rules: vec![rule(
                    "admin",
                    Resource::Topic,
                    Pattern::Literal,
                    "x",
                    &[Operation::Read],
                    Permission::Allow
                )]
            }
            .check()
            .is_err()
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acl.json");
        std::fs::write(&path, r#"{"rules":[]}"#).unwrap();
        let a = Authorizer::from_file(&path).unwrap();
        let u = Principal::user("u");
        assert!(!a.allows(&u, host(), Resource::Topic, "x", Operation::Read));
        std::thread::sleep(Duration::from_millis(1100));
        std::fs::write(
            &path,
            r#"{"rules":[{"principal":"User:u","resource":"topic","name":"x","operations":["read"],"permission":"allow"}]}"#,
        )
        .unwrap();
        a.checked.lock().0 = Instant::now().checked_sub(RECHECK).unwrap();
        assert!(a.allows(&u, host(), Resource::Topic, "x", Operation::Read));
    }
}
