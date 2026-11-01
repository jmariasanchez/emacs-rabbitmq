//! Credential redaction.
//!
//! Applied to every error message, log line, audit record, and diagnostic
//! report that leaves the bridge. Redaction is conservative: when in doubt,
//! the value is masked.

use serde_json::Value;

pub const MASK: &str = "********";

/// Keys whose values are always masked in structured data.
const SECRET_KEYS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "authorization",
    "password_hash",
    "client_key",
    "private_key",
    "key_password",
    "pkcs12_password",
];

pub fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    SECRET_KEYS
        .iter()
        .any(|s| k == *s || k.ends_with(&format!("_{s}")))
}

/// Mask `user:password@` userinfo inside any URI-looking substring, plus
/// `password=...` query parameters.
pub fn redact_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(idx) = rest.find("://") {
        let (head, tail) = rest.split_at(idx + 3);
        out.push_str(head);
        // Authority runs until '/', whitespace, quote, or end.
        let end = tail
            .find(|c: char| c == '/' || c.is_whitespace() || c == '"' || c == '\'' || c == '>')
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        if let Some(at) = authority.rfind('@') {
            let userinfo = &authority[..at];
            let user = userinfo.split(':').next().unwrap_or("");
            if userinfo.contains(':') {
                out.push_str(user);
                out.push(':');
                out.push_str(MASK);
            } else {
                out.push_str(user);
            }
            out.push_str(&authority[at..]);
        } else {
            out.push_str(authority);
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    redact_query_password(&out)
}

fn redact_query_password(input: &str) -> String {
    let lower = input.to_ascii_lowercase();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while let Some(off) = lower[i..].find("password=") {
        let start = i + off + "password=".len();
        out.push_str(&input[i..start]);
        let end = input[start..]
            .find(|c: char| c == '&' || c.is_whitespace() || c == '"')
            .map(|e| start + e)
            .unwrap_or(input.len());
        if end > start {
            out.push_str(MASK);
        }
        i = end;
    }
    out.push_str(&input[i..]);
    out
}

/// Recursively redact a JSON value: secret keys are masked and strings are
/// passed through [`redact_text`].
pub fn redact_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    if is_secret_key(k) && !v.is_null() {
                        (k.clone(), Value::String(MASK.into()))
                    } else {
                        (k.clone(), redact_json(v))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact_json).collect()),
        Value::String(s) => Value::String(redact_text(s)),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn uri_userinfo() {
        assert_eq!(
            redact_text("amqp://guest:guest@localhost:5672/%2f"),
            format!("amqp://guest:{MASK}@localhost:5672/%2f")
        );
        assert_eq!(redact_text("amqp://localhost"), "amqp://localhost");
        assert_eq!(redact_text("amqps://u@h/v"), "amqps://u@h/v");
        let two = redact_text("a amqp://a:b@h and https://c:d@e/x");
        assert!(!two.contains(":b@") && !two.contains(":d@"), "{two}");
    }

    #[test]
    fn query_password() {
        let s = redact_text("https://h/api?user=x&password=abc&z=1");
        assert!(!s.contains("abc"));
        assert!(s.contains("z=1"));
    }

    #[test]
    fn json_keys() {
        let v = redact_json(&json!({
            "username": "bob",
            "password": "p",
            "nested": [{"client_key_password": "k", "uri": "amqp://a:b@c"}],
            "password_hash": null
        }));
        assert_eq!(v["username"], "bob");
        assert_eq!(v["password"], MASK);
        assert_eq!(v["nested"][0]["client_key_password"], MASK);
        assert!(!v["nested"][0]["uri"].as_str().unwrap().contains(":b@"));
        assert!(v["password_hash"].is_null());
    }
}
