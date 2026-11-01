//! Observation vs. intervention safety model.
//!
//! Every RPC method declares an [`Effect`]. Before an adapter is invoked the
//! bridge asks the profile's [`SafetyPolicy`] whether the effect is allowed on
//! the target [`Resource`]. The rules, in order:
//!
//! 1. `Observe` is always allowed (metadata reads, stream replays, routing
//!    predictions).
//! 2. A read-only profile rejects every other effect with `read_only_profile`.
//! 3. Protected-resource rules may deny writes or destructive operations on
//!    matching resources.
//! 4. `Consume` (fetching from an ordinary queue, which alters queue state)
//!    requires explicit `consent`.
//! 5. `Destructive` effects require the caller to echo the target resource
//!    name in `confirm`. This mirrors a typed confirmation in the UI and
//!    prevents a stray keypress or a replayed request from deleting the wrong
//!    entity.

use crate::error::{CoreError, ErrorCode, Result};
use serde::{Deserialize, Serialize};

/// What an operation does to broker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// No broker state change. Includes stream reads (offset based) and
    /// Management API GETs.
    Observe,
    /// Removes or reorders messages in an ordinary queue (basic.get,
    /// basic.consume, AMQP 1.0 receive). Requires consent.
    Consume,
    /// Settles a delivery (ack / nack / reject / AMQP 1.0 disposition).
    Settle,
    /// Adds messages (publish / stream send).
    Publish,
    /// Creates or changes topology or configuration without removing data.
    Write,
    /// Removes data or entities (delete, purge, permission revocation,
    /// definitions import that may overwrite). Requires confirmation.
    Destructive,
}

impl Effect {
    pub fn is_intervention(self) -> bool {
        self != Effect::Observe
    }
}

/// The resource an operation targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resource {
    /// `queue`, `exchange`, `binding`, `stream`, `vhost`, `user`, `policy`,
    /// `connection`, `definitions`, ...
    pub kind: String,
    #[serde(default)]
    pub vhost: Option<String>,
    pub name: String,
}

impl Resource {
    pub fn new(kind: &str, vhost: Option<&str>, name: &str) -> Self {
        Self {
            kind: kind.into(),
            vhost: vhost.map(Into::into),
            name: name.into(),
        }
    }
}

/// How strongly a protected resource is guarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protection {
    /// Reject destructive effects; allow publish/write/consume.
    NoDestructive,
    /// Reject every intervention.
    ReadOnly,
}

/// Glob-based protected-resource rule. `*` matches any run of characters,
/// `?` a single character. Missing fields match anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectedRule {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub vhost: Option<String>,
    pub name: String,
    #[serde(default = "default_protection")]
    pub protection: Protection,
}

fn default_protection() -> Protection {
    Protection::NoDestructive
}

impl ProtectedRule {
    pub fn matches(&self, r: &Resource) -> bool {
        self.kind.as_deref().is_none_or(|k| glob_match(k, &r.kind))
            && self
                .vhost
                .as_deref()
                .is_none_or(|v| r.vhost.as_deref().is_some_and(|rv| glob_match(v, rv)))
            && glob_match(&self.name, &r.name)
    }
}

/// Minimal glob matcher (`*`, `?`). Iterative with backtracking on the last star.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// A request for permission to perform an operation.
#[derive(Debug, Clone)]
pub struct Intent<'a> {
    pub method: &'a str,
    pub effect: Effect,
    pub resource: Option<Resource>,
    /// The user explicitly consented to a state-altering read.
    pub consent: bool,
    /// Typed confirmation; must equal `resource.name` for destructive effects.
    pub confirm: Option<&'a str>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SafetyPolicy {
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub protected: Vec<ProtectedRule>,
}

impl SafetyPolicy {
    pub fn check(&self, intent: &Intent<'_>) -> Result<()> {
        if intent.effect == Effect::Observe {
            return Ok(());
        }
        if self.read_only {
            return Err(CoreError::new(
                ErrorCode::ReadOnlyProfile,
                format!(
                    "{} is not permitted: the profile is read-only",
                    intent.method
                ),
            )
            .with_hint("Use a profile without :read-only to perform interventions."));
        }
        if let Some(res) = &intent.resource {
            for rule in &self.protected {
                if !rule.matches(res) {
                    continue;
                }
                let denied = match rule.protection {
                    Protection::ReadOnly => true,
                    Protection::NoDestructive => intent.effect == Effect::Destructive,
                };
                if denied {
                    return Err(CoreError::new(
                        ErrorCode::ProtectedResource,
                        format!(
                            "{} on {} '{}' is blocked by a protected-resource rule",
                            intent.method, res.kind, res.name
                        ),
                    )
                    .with_detail(serde_json::to_value(rule).unwrap_or_default()));
                }
            }
        }
        if intent.effect == Effect::Consume && !intent.consent {
            return Err(CoreError::new(
                ErrorCode::ConfirmationRequired,
                format!(
                    "{} removes messages from an ordinary queue and may change ordering and redelivery state; pass consent=true",
                    intent.method
                ),
            )
            .with_detail(serde_json::json!({ "requires": "consent" })));
        }
        if intent.effect == Effect::Destructive {
            let expected = intent.resource.as_ref().map(|r| r.name.as_str());
            match (expected, intent.confirm) {
                (Some(exp), Some(got)) if exp == got => {}
                (None, Some(_)) => {}
                _ => {
                    return Err(CoreError::new(
                        ErrorCode::ConfirmationRequired,
                        format!(
                            "{} is destructive; confirm by passing confirm=\"{}\"",
                            intent.method,
                            expected.unwrap_or("yes")
                        ),
                    )
                    .with_detail(serde_json::json!({
                        "requires": "confirm",
                        "expected": expected.unwrap_or("yes"),
                    })));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(effect: Effect, name: &str) -> Intent<'static> {
        Intent {
            method: "test",
            effect,
            resource: Some(Resource::new(
                "queue",
                Some("/"),
                Box::leak(name.to_string().into_boxed_str()),
            )),
            consent: false,
            confirm: None,
        }
    }

    #[test]
    fn glob() {
        assert!(glob_match("*", ""));
        assert!(glob_match("prod.*", "prod.orders"));
        assert!(!glob_match("prod.*", "dev.orders"));
        assert!(glob_match("a?c", "abc"));
        assert!(glob_match("*.dlq", "orders.dlq"));
        assert!(glob_match("a*b*c", "axxbyyc"));
        assert!(!glob_match("a*b*c", "axxbyy"));
    }

    #[test]
    fn observe_always_allowed() {
        let p = SafetyPolicy {
            read_only: true,
            protected: vec![],
        };
        assert!(p.check(&intent(Effect::Observe, "q")).is_ok());
    }

    #[test]
    fn read_only_blocks_interventions() {
        let p = SafetyPolicy {
            read_only: true,
            protected: vec![],
        };
        for e in [
            Effect::Consume,
            Effect::Settle,
            Effect::Publish,
            Effect::Write,
            Effect::Destructive,
        ] {
            assert_eq!(
                p.check(&intent(e, "q")).unwrap_err().code,
                ErrorCode::ReadOnlyProfile
            );
        }
    }

    #[test]
    fn consume_requires_consent() {
        let p = SafetyPolicy::default();
        let mut i = intent(Effect::Consume, "q");
        assert_eq!(
            p.check(&i).unwrap_err().code,
            ErrorCode::ConfirmationRequired
        );
        i.consent = true;
        assert!(p.check(&i).is_ok());
    }

    #[test]
    fn destructive_requires_matching_confirm() {
        let p = SafetyPolicy::default();
        let mut i = intent(Effect::Destructive, "orders");
        assert!(p.check(&i).is_err());
        i.confirm = Some("other");
        assert!(p.check(&i).is_err());
        i.confirm = Some("orders");
        assert!(p.check(&i).is_ok());
    }

    #[test]
    fn protected_rules() {
        let p = SafetyPolicy {
            read_only: false,
            protected: vec![
                ProtectedRule {
                    kind: Some("queue".into()),
                    vhost: None,
                    name: "prod.*".into(),
                    protection: Protection::NoDestructive,
                },
                ProtectedRule {
                    kind: None,
                    vhost: Some("/".into()),
                    name: "audit".into(),
                    protection: Protection::ReadOnly,
                },
            ],
        };
        let mut i = intent(Effect::Destructive, "prod.orders");
        i.confirm = Some("prod.orders");
        assert_eq!(p.check(&i).unwrap_err().code, ErrorCode::ProtectedResource);
        assert!(p.check(&intent(Effect::Publish, "prod.orders")).is_ok());
        assert_eq!(
            p.check(&intent(Effect::Publish, "audit")).unwrap_err().code,
            ErrorCode::ProtectedResource
        );
    }
}
