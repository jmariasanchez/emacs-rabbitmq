//! Conversion between AMQP 0-9-1 field tables / properties and the
//! protocol-neutral JSON model in `rabbitmq-core`.
//!
//! JSON has fewer types than AMQP field tables. Inbound values are mapped to
//! natural JSON where lossless (bool, integers, floats, strings, arrays,
//! tables) and to tagged objects otherwise:
//!
//! * timestamp: `{"$type": "timestamp", "value": <seconds>}`
//! * byte array / non-UTF-8 long string: `{"$type": "bytes", "base64": "..."}`
//! * decimal: `{"$type": "decimal", "scale": s, "value": v}`
//!
//! Outbound, the same tagged objects are accepted, plus explicit integer
//! widths (`{"$type": "i8"|"u8"|"i16"|"u16"|"i32"|"u32"|"i64"|"f32"|"f64", "value": n}`).
//! Untagged integers become `i64` (`l`) which RabbitMQ accepts everywhere an
//! integer argument is expected.

use lapin::types::{
    AMQPValue, ByteArray, DecimalValue, FieldArray, FieldTable, LongString, ShortString,
};
use lapin::BasicProperties;
use rabbitmq_core::error::{CoreError, Result};
use rabbitmq_core::message::{decode_b64, encode_b64, MessageProperties};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub fn amqp_to_json(v: &AMQPValue) -> Value {
    match v {
        AMQPValue::Boolean(b) => Value::Bool(*b),
        AMQPValue::ShortShortInt(n) => json!(n),
        AMQPValue::ShortShortUInt(n) => json!(n),
        AMQPValue::ShortInt(n) => json!(n),
        AMQPValue::ShortUInt(n) => json!(n),
        AMQPValue::LongInt(n) => json!(n),
        AMQPValue::LongUInt(n) => json!(n),
        AMQPValue::LongLongInt(n) => json!(n),
        AMQPValue::Float(f) => json!(f),
        AMQPValue::Double(f) => json!(f),
        AMQPValue::DecimalValue(d) => {
            json!({"$type": "decimal", "scale": d.scale, "value": d.value})
        }
        AMQPValue::ShortString(s) => Value::String(s.as_str().to_string()),
        AMQPValue::LongString(s) => match std::str::from_utf8(s.as_bytes()) {
            Ok(t) => Value::String(t.to_string()),
            Err(_) => json!({"$type": "bytes", "base64": encode_b64(s.as_bytes())}),
        },
        AMQPValue::FieldArray(a) => Value::Array(a.as_slice().iter().map(amqp_to_json).collect()),
        AMQPValue::Timestamp(t) => json!({"$type": "timestamp", "value": t}),
        AMQPValue::FieldTable(t) => Value::Object(table_to_json(t)),
        AMQPValue::ByteArray(b) => json!({"$type": "bytes", "base64": encode_b64(b.as_slice())}),
        AMQPValue::Void => Value::Null,
    }
}

pub fn table_to_json(t: &FieldTable) -> Map<String, Value> {
    t.inner()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), amqp_to_json(v)))
        .collect()
}

pub fn json_to_amqp(v: &Value) -> Result<AMQPValue> {
    Ok(match v {
        Value::Null => AMQPValue::Void,
        Value::Bool(b) => AMQPValue::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                AMQPValue::LongLongInt(i)
            } else if let Some(u) = n.as_u64() {
                // > i64::MAX: no unsigned 64-bit type in RabbitMQ field tables.
                return Err(CoreError::invalid(format!(
                    "integer {u} does not fit a signed 64-bit field"
                )));
            } else {
                AMQPValue::Double(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => AMQPValue::LongString(LongString::from(s.as_bytes().to_vec())),
        Value::Array(a) => AMQPValue::FieldArray(FieldArray::from(
            a.iter().map(json_to_amqp).collect::<Result<Vec<_>>>()?,
        )),
        Value::Object(m) => {
            if let Some(t) = m.get("$type").and_then(Value::as_str) {
                return typed(t, m);
            }
            AMQPValue::FieldTable(json_to_table(m)?)
        }
    })
}

fn typed(t: &str, m: &Map<String, Value>) -> Result<AMQPValue> {
    let val = m.get("value");
    let int = || {
        val.and_then(Value::as_i64)
            .ok_or_else(|| CoreError::invalid(format!("{t} value must be an integer")))
    };
    let range = |n: i64, lo: i64, hi: i64| {
        if n < lo || n > hi {
            Err(CoreError::invalid(format!("{n} is out of range for {t}")))
        } else {
            Ok(n)
        }
    };
    Ok(match t {
        "timestamp" => AMQPValue::Timestamp(val.and_then(Value::as_u64).ok_or_else(|| {
            CoreError::invalid("timestamp value must be a non-negative integer (seconds)")
        })?),
        "bytes" => {
            let b = m
                .get("base64")
                .and_then(Value::as_str)
                .ok_or_else(|| CoreError::invalid("bytes requires a base64 field"))?;
            AMQPValue::ByteArray(ByteArray::from(decode_b64(b)?))
        }
        "decimal" => AMQPValue::DecimalValue(DecimalValue {
            scale: m.get("scale").and_then(Value::as_u64).unwrap_or(0) as u8,
            value: int()? as u32,
        }),
        "i8" => AMQPValue::ShortShortInt(range(int()?, i8::MIN as i64, i8::MAX as i64)? as i8),
        "u8" => AMQPValue::ShortShortUInt(range(int()?, 0, u8::MAX as i64)? as u8),
        "i16" => AMQPValue::ShortInt(range(int()?, i16::MIN as i64, i16::MAX as i64)? as i16),
        "u16" => AMQPValue::ShortUInt(range(int()?, 0, u16::MAX as i64)? as u16),
        "i32" => AMQPValue::LongInt(range(int()?, i32::MIN as i64, i32::MAX as i64)? as i32),
        "u32" => AMQPValue::LongUInt(range(int()?, 0, u32::MAX as i64)? as u32),
        "i64" => AMQPValue::LongLongInt(int()?),
        "f32" => AMQPValue::Float(val.and_then(Value::as_f64).unwrap_or(0.0) as f32),
        "f64" => AMQPValue::Double(val.and_then(Value::as_f64).unwrap_or(0.0)),
        "string" => AMQPValue::LongString(LongString::from(
            val.and_then(Value::as_str)
                .unwrap_or_default()
                .as_bytes()
                .to_vec(),
        )),
        other => return Err(CoreError::invalid(format!("unknown $type \"{other}\""))),
    })
}

pub fn json_to_table(m: &Map<String, Value>) -> Result<FieldTable> {
    let mut bt = BTreeMap::new();
    for (k, v) in m {
        let key = ShortString::try_new(k.clone())
            .map_err(|_| CoreError::invalid(format!("field name \"{k}\" exceeds 255 bytes")))?;
        bt.insert(key, json_to_amqp(v)?);
    }
    Ok(FieldTable::from(bt))
}

fn ss(s: &str, what: &str) -> Result<ShortString> {
    ShortString::try_new(s.to_string())
        .map_err(|_| CoreError::invalid(format!("{what} exceeds 255 bytes")))
}

pub fn to_basic_properties(p: &MessageProperties) -> Result<BasicProperties> {
    p.validate()?;
    let mut b = BasicProperties::default();
    if let Some(v) = &p.content_type {
        b = b.with_content_type(ss(v, "content_type")?);
    }
    if let Some(v) = &p.content_encoding {
        b = b.with_content_encoding(ss(v, "content_encoding")?);
    }
    if !p.headers.is_empty() {
        b = b.with_headers(json_to_table(&p.headers)?);
    }
    if let Some(v) = p.delivery_mode {
        b = b.with_delivery_mode(v);
    }
    if let Some(v) = p.priority {
        b = b.with_priority(v);
    }
    if let Some(v) = &p.correlation_id {
        b = b.with_correlation_id(ss(v, "correlation_id")?);
    }
    if let Some(v) = &p.reply_to {
        b = b.with_reply_to(ss(v, "reply_to")?);
    }
    if let Some(v) = &p.expiration {
        b = b.with_expiration(ss(v, "expiration")?);
    }
    if let Some(v) = &p.message_id {
        b = b.with_message_id(ss(v, "message_id")?);
    }
    if let Some(v) = p.timestamp {
        b = b.with_timestamp(v);
    }
    if let Some(v) = &p.kind {
        b = b.with_type(ss(v, "type")?);
    }
    if let Some(v) = &p.user_id {
        b = b.with_user_id(ss(v, "user_id")?);
    }
    if let Some(v) = &p.app_id {
        b = b.with_app_id(ss(v, "app_id")?);
    }
    Ok(b)
}

pub fn from_basic_properties(b: &BasicProperties) -> MessageProperties {
    let s = |o: &Option<ShortString>| o.as_ref().map(|v| v.as_str().to_string());
    MessageProperties {
        content_type: s(b.content_type()),
        content_encoding: s(b.content_encoding()),
        headers: b.headers().as_ref().map(table_to_json).unwrap_or_default(),
        delivery_mode: *b.delivery_mode(),
        priority: *b.priority(),
        correlation_id: s(b.correlation_id()),
        reply_to: s(b.reply_to()),
        expiration: s(b.expiration()),
        message_id: s(b.message_id()),
        timestamp: *b.timestamp(),
        kind: s(b.kind()),
        user_id: s(b.user_id()),
        app_id: s(b.app_id()),
        annotations: Map::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_roundtrip() {
        let j = json!({
            "a": 1, "b": true, "c": "s", "d": [1, "x"], "e": {"n": 2.5},
            "ts": {"$type": "timestamp", "value": 1700000000},
            "bin": {"$type": "bytes", "base64": "AAEC"},
            "small": {"$type": "u8", "value": 7},
            "nil": null
        });
        let t = json_to_table(j.as_object().unwrap()).unwrap();
        assert!(matches!(
            t.inner().get("small"),
            Some(AMQPValue::ShortShortUInt(7))
        ));
        let back = Value::Object(table_to_json(&t));
        assert_eq!(back["a"], 1);
        assert_eq!(back["ts"]["value"], 1700000000);
        assert_eq!(back["bin"]["base64"], "AAEC");
        assert_eq!(back["d"][1], "x");
        assert_eq!(back["e"]["n"], 2.5);
        assert!(back["nil"].is_null());
    }

    #[test]
    fn typed_ranges() {
        assert!(json_to_amqp(&json!({"$type": "u8", "value": 300})).is_err());
        assert!(json_to_amqp(&json!({"$type": "wat"})).is_err());
        assert!(json_to_amqp(&json!(u64::MAX)).is_err());
        let long = "k".repeat(300);
        let mut m = Map::new();
        m.insert(long, json!(1));
        assert!(json_to_table(&m).is_err());
    }

    #[test]
    fn properties_roundtrip() {
        let p: MessageProperties = serde_json::from_value(json!({
            "content_type": "application/json", "delivery_mode": 2, "priority": 3,
            "correlation_id": "c", "reply_to": "r", "expiration": "1000", "message_id": "m",
            "timestamp": 5, "type": "t", "app_id": "a", "headers": {"h": 1}
        }))
        .unwrap();
        let b = to_basic_properties(&p).unwrap();
        let back = from_basic_properties(&b);
        assert_eq!(back, p);
    }
}
