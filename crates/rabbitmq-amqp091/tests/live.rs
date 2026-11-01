//! Live AMQP 0-9-1 tests. Each test targets a documented safety property.

use rabbitmq_amqp091::session::*;
use rabbitmq_core::capabilities::{QueueSpec, QueueType};
use rabbitmq_core::events::{names, MemorySink};
use rabbitmq_core::message::*;
use rabbitmq_core::ErrorCode;
use rabbitmq_testkit::{broker, unique, Admin};
use serde_json::{json, Map};
use std::sync::Arc;
use std::time::Duration;

const T: Duration = Duration::from_secs(10);

fn spec(name: &str, qt: QueueType) -> QueueSpec {
    QueueSpec {
        name: name.into(),
        queue_type: qt,
        durable: true,
        exclusive: false,
        auto_delete: false,
        arguments: Map::new(),
    }
}

fn msg(rk: &str, body: &str) -> OutboundMessage {
    OutboundMessage {
        exchange: String::new(),
        routing_key: rk.into(),
        payload: Payload::text(body),
        ..Default::default()
    }
}

async fn session(name: &str) -> Option<(Arc<Session>, Arc<MemorySink>, Admin)> {
    let mut p = broker().await?;
    p.connection_name = Some(name.into());
    let sink = MemorySink::new();
    let s = Session::connect(name.into(), p.clone(), sink.clone())
        .await
        .expect("connect");
    Some((s, sink, Admin::new(&p)))
}

#[tokio::test]
async fn publish_confirm_and_get_with_manual_ack() {
    let Some((s, _sink, admin)) = session(&unique("conn")).await else {
        return;
    };
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Quorum), false)
        .await
        .unwrap();

    let mut m = msg(&q, "hello");
    m.properties.content_type = Some("text/plain".into());
    m.properties.headers.insert("k".into(), json!("v"));
    m.properties.delivery_mode = Some(2);
    let out = s
        .publish(&[m.clone(), msg(&q, "world")], PublishMode::Confirm, T)
        .await
        .unwrap();
    assert!(
        out.iter().all(|o| o.status == PublishStatus::Confirmed),
        "{out:?}"
    );

    let got = s.get(&q, 1, false).await.unwrap();
    assert!(got.destructive);
    let first = &got.messages[0];
    assert_eq!(first.payload.data, "hello");
    assert_eq!(first.properties.headers["k"], "v");
    assert_eq!(first.properties.content_type.as_deref(), Some("text/plain"));
    assert_eq!(first.message_count, Some(1));
    assert!(!first.redelivered);

    // The message is unacked, not gone.
    admin
        .wait_queue(&q, |v| v["messages_unacknowledged"] == 1)
        .await;
    let r = s.settle(&first.id, Settlement::Ack, false).await.unwrap();
    assert!(r[0].settled);
    // Double settlement is rejected rather than silently acking something else.
    assert_eq!(
        s.settle(&first.id, Settlement::Ack, false)
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    admin
        .wait_queue(&q, |v| {
            v["messages"] == 1 && v["messages_unacknowledged"] == 0
        })
        .await;

    // Nack with requeue: message comes back marked redelivered.
    let got = s.get(&q, 1, false).await.unwrap();
    s.settle(
        &got.messages[0].id,
        Settlement::Nack { requeue: true },
        false,
    )
    .await
    .unwrap();
    let again = s.get(&q, 1, true).await.unwrap();
    assert!(again.messages[0].redelivered);
    assert_eq!(again.messages[0].payload.data, "world");
    // auto-acked deliveries are not settleable.
    assert_eq!(
        s.settle(&again.messages[0].id, Settlement::Ack, false)
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    assert!(s.get(&q, 1, false).await.unwrap().empty);

    s.delete_queue(&q, false, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn mandatory_unroutable_is_returned_not_confirmed() {
    let Some((s, sink, _)) = session(&unique("conn")).await else {
        return;
    };
    let x = unique("x");
    s.declare_exchange(&serde_json::from_value(json!({"name": x, "type": "direct"})).unwrap())
        .await
        .unwrap();
    let mut m = msg("nowhere", "lost?");
    m.exchange = x.clone();
    m.mandatory = true;
    let mut plain = m.clone();
    plain.mandatory = false;
    let out = s
        .publish(&[m, plain], PublishMode::Confirm, T)
        .await
        .unwrap();
    assert_eq!(out[0].status, PublishStatus::Returned, "{out:?}");
    assert_eq!(out[0].reply_code, Some(312));
    // Without mandatory the broker confirms and drops it: still "confirmed",
    // which is why the UI pairs confirms with routing predictions.
    assert_eq!(out[1].status, PublishStatus::Confirmed);
    assert_eq!(sink.count(names::RETURNED), 1);
    s.delete_exchange(&x, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn publish_to_missing_exchange_is_not_reported_confirmed() {
    let Some((s, _, _)) = session(&unique("conn")).await else {
        return;
    };
    let mut m = msg("k", "x");
    m.exchange = unique("missing-x");
    let out = s
        .publish(&[m], PublishMode::Confirm, Duration::from_secs(3))
        .await
        .unwrap();
    assert_ne!(out[0].status, PublishStatus::Confirmed, "{out:?}");
    // The publish channel was closed by the broker (404); the next publish must
    // transparently use a fresh channel.
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Classic), false)
        .await
        .unwrap();
    let out = s
        .publish(&[msg(&q, "ok")], PublishMode::Confirm, T)
        .await
        .unwrap();
    assert_eq!(out[0].status, PublishStatus::Confirmed);
    s.delete_queue(&q, false, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn transactions_commit() {
    let Some((s, _, admin)) = session(&unique("conn")).await else {
        return;
    };
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Classic), false)
        .await
        .unwrap();
    let out = s
        .publish(&[msg(&q, "a"), msg(&q, "b")], PublishMode::Transaction, T)
        .await
        .unwrap();
    assert!(out.iter().all(|o| o.status == PublishStatus::Committed));
    admin.wait_queue(&q, |v| v["messages"] == 2).await;
    let out = s
        .publish(&[msg(&q, "c")], PublishMode::NoConfirm, T)
        .await
        .unwrap();
    assert_eq!(out[0].status, PublishStatus::Unconfirmed);
    s.delete_queue(&q, false, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn redeclare_with_different_args_fails_without_changing_definition() {
    let Some((s, _, admin)) = session(&unique("conn")).await else {
        return;
    };
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Quorum), false)
        .await
        .unwrap();
    let e = s
        .declare_queue(spec(&q, QueueType::Classic), false)
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::PreconditionFailed, "{e:?}");
    assert!(e.hint.is_some());
    let info = admin
        .get(&format!("queues/%2F/{}", rabbitmq_testkit::pct(&q)))
        .await;
    assert_eq!(info["type"], "quorum");
    // The session is still usable after the channel-level error.
    let ok = s
        .declare_queue(spec(&q, QueueType::Quorum), true)
        .await
        .unwrap();
    assert_eq!(ok.name, q);
    // Local validation catches documented incompatibilities before sending.
    let mut bad = spec(&q, QueueType::Quorum);
    bad.arguments.insert("x-max-priority".into(), json!(5));
    assert_eq!(
        s.declare_queue(bad, false).await.unwrap_err().code,
        ErrorCode::Unsupported
    );
    s.delete_queue(&q, false, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn bindings_and_exchange_to_exchange() {
    let Some((s, _, _)) = session(&unique("conn")).await else {
        return;
    };
    let (x1, x2, q) = (unique("x1"), unique("x2"), unique("q"));
    for (x, t) in [(&x1, "topic"), (&x2, "fanout")] {
        s.declare_exchange(&serde_json::from_value(json!({"name": x, "type": t})).unwrap())
            .await
            .unwrap();
    }
    s.declare_queue(spec(&q, QueueType::Classic), false)
        .await
        .unwrap();
    s.bind("exchange", &x2, &x1, "a.#", &Map::new(), false)
        .await
        .unwrap();
    s.bind("queue", &q, &x2, "", &Map::new(), false)
        .await
        .unwrap();
    let mut m = msg("a.b", "via e2e");
    m.exchange = x1.clone();
    m.mandatory = true;
    let out = s
        .publish(&[m.clone()], PublishMode::Confirm, T)
        .await
        .unwrap();
    assert_eq!(out[0].status, PublishStatus::Confirmed);
    assert_eq!(
        s.get(&q, 1, true).await.unwrap().messages[0].payload.data,
        "via e2e"
    );
    s.bind("exchange", &x2, &x1, "a.#", &Map::new(), true)
        .await
        .unwrap();
    let out = s.publish(&[m], PublishMode::Confirm, T).await.unwrap();
    assert_eq!(out[0].status, PublishStatus::Returned);
    s.delete_queue(&q, false, false).await.unwrap();
    s.delete_exchange(&x1, false).await.unwrap();
    s.delete_exchange(&x2, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn consumer_manual_ack_prefetch_and_cancel() {
    let Some((s, sink, admin)) = session(&unique("conn")).await else {
        return;
    };
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Quorum), false)
        .await
        .unwrap();
    let msgs: Vec<_> = (0..20).map(|i| msg(&q, &format!("m{i}"))).collect();
    s.publish(&msgs, PublishMode::Confirm, T).await.unwrap();

    let c = s
        .consume(
            serde_json::from_value(json!({"queue": q, "prefetch": 5, "flush_ms": 20})).unwrap(),
        )
        .await
        .unwrap();
    assert!(!c.auto_ack, "manual ack must be the default");
    // Prefetch bounds what the broker pushes before we settle.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let delivered: Vec<InboundMessage> = sink
        .snapshot()
        .into_iter()
        .filter(|(m, _)| m == names::DELIVERIES)
        .flat_map(|(_, p)| {
            serde_json::from_value::<Vec<InboundMessage>>(p["messages"].clone()).unwrap()
        })
        .collect();
    assert_eq!(delivered.len(), 5, "prefetch=5 must cap unacked deliveries");
    for d in &delivered {
        s.settle(&d.id, Settlement::Ack, false).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let more = s.unsettled_ids().await;
    assert_eq!(more.len(), 5);
    // `multiple` settles everything up to and including the given tag on that channel only.
    let last = more
        .iter()
        .max_by_key(|id| id.rsplit('/').next().unwrap().parse::<u64>().unwrap())
        .unwrap();
    let r = s.settle(last, Settlement::Ack, true).await.unwrap();
    assert_eq!(r.len(), 5);

    let info = s.cancel(&c.tag).await.unwrap();
    assert_eq!(info.state, "cancelled");
    admin
        .wait_queue(&q, |v| {
            v["consumers"] == 0 && v["messages_unacknowledged"] == 0
        })
        .await;
    let left = admin
        .wait_queue(&q, |v| v["messages"].as_u64().is_some())
        .await;
    assert!(left["messages"].as_u64().unwrap() <= 10);
    s.delete_queue(&q, false, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn queue_deleted_while_consuming_invalidates_deliveries() {
    let Some((s, sink, admin)) = session(&unique("conn")).await else {
        return;
    };
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Classic), false)
        .await
        .unwrap();
    s.publish(&[msg(&q, "a")], PublishMode::Confirm, T)
        .await
        .unwrap();
    let c = s
        .consume(serde_json::from_value(json!({"queue": q, "flush_ms": 20})).unwrap())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let id = s
        .unsettled_ids()
        .await
        .pop()
        .expect("one unsettled delivery");
    admin.delete_queue(&q).await;
    // Broker sends basic.cancel; the consumer is reported as cancelled by the broker.
    for _ in 0..40 {
        if sink
            .snapshot()
            .iter()
            .any(|(m, p)| m == names::CONSUMER && p["state"] == "cancelled_by_broker")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(sink.snapshot().iter().any(|(m, p)| m == names::CONSUMER
        && p["consumer"] == c.tag.as_str()
        && p["state"] == "cancelled_by_broker"));
    let e = s.settle(&id, Settlement::Ack, false).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::StaleDelivery);
    s.close().await.unwrap();
}

#[tokio::test]
async fn connection_loss_invalidates_unacked_and_reconnects_without_resubscribing() {
    let name = unique("conn-kill");
    let Some((s, sink, admin)) = session(&name).await else {
        return;
    };
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Quorum), false)
        .await
        .unwrap();
    s.publish(&[msg(&q, "a"), msg(&q, "b")], PublishMode::Confirm, T)
        .await
        .unwrap();
    let got = s.get(&q, 2, false).await.unwrap();
    assert_eq!(got.messages.len(), 2);
    let c = s
        .consume(serde_json::from_value(json!({"queue": q})).unwrap())
        .await
        .unwrap();

    admin.wait_connection(&name).await;
    assert_eq!(admin.close_connections_named(&name).await, 1);

    // Wait for the session to reconnect (epoch 2).
    let mut reconnected = false;
    for _ in 0..80 {
        let info = s.info().await;
        if info.epoch >= 2 && info.state == SessionState::Connected {
            reconnected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reconnected, "{:?}", s.info().await);
    let lost = sink
        .snapshot()
        .into_iter()
        .find(|(m, p)| m == names::SESSION && p["state"] == "lost")
        .expect("lost event");
    assert_eq!(lost.1["detail"]["invalidated_deliveries"], 2);
    for m in &got.messages {
        assert_eq!(
            s.settle(&m.id, Settlement::Ack, false)
                .await
                .unwrap_err()
                .code,
            ErrorCode::StaleDelivery
        );
    }
    // The consumer is not silently resumed.
    let info = s.info().await;
    assert!(info
        .consumers
        .iter()
        .any(|x| x.tag == c.tag && x.state == "lost"));
    // Broker requeued both messages; they come back redelivered.
    let again = s.get(&q, 2, true).await.unwrap();
    assert_eq!(again.messages.len(), 2);
    assert!(again.messages.iter().all(|m| m.redelivered));
    s.delete_queue(&q, false, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn auth_failure_is_classified_and_redacted() {
    let Some(mut p) = broker().await else { return };
    p.password = rabbitmq_core::profile::Secret::new("definitely-wrong");
    p.reconnect_attempts = 0;
    let e = Session::connect("bad".into(), p, MemorySink::new())
        .await
        .err()
        .expect("must fail");
    assert!(
        matches!(
            e.code,
            ErrorCode::AuthenticationFailed
                | ErrorCode::AccessRefused
                | ErrorCode::ConnectionFailed
        ),
        "{e:?}"
    );
    assert!(!e.message.contains("definitely-wrong"), "{}", e.message);
}

#[tokio::test]
async fn binary_and_large_payloads() {
    let Some((s, _, _)) = session(&unique("conn")).await else {
        return;
    };
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Classic), false)
        .await
        .unwrap();
    let bin: Vec<u8> = (0..=255u8).collect();
    let big = "x".repeat(200 * 1024);
    let m1 = OutboundMessage {
        routing_key: q.clone(),
        payload: Payload {
            encoding: PayloadEncoding::Base64,
            data: encode_b64(&bin),
        },
        ..Default::default()
    };
    let out = s
        .publish(&[m1, msg(&q, &big)], PublishMode::Confirm, T)
        .await
        .unwrap();
    assert!(out.iter().all(|o| o.status == PublishStatus::Confirmed));
    let got = s.get(&q, 2, true).await.unwrap();
    let b = &got.messages[0].payload;
    assert_eq!(b.encoding, PayloadEncoding::Base64);
    assert_eq!(decode_b64(&b.data).unwrap(), bin);
    let l = &got.messages[1].payload;
    assert!(l.truncated);
    assert_eq!(l.size, 200 * 1024);
    assert_eq!(l.data.len(), 64 * 1024);
    s.delete_queue(&q, false, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn purge_and_conditional_delete() {
    let Some((s, _, _)) = session(&unique("conn")).await else {
        return;
    };
    let q = unique("q");
    s.declare_queue(spec(&q, QueueType::Classic), false)
        .await
        .unwrap();
    s.publish(&[msg(&q, "a"), msg(&q, "b")], PublishMode::Confirm, T)
        .await
        .unwrap();
    let e = s.delete_queue(&q, false, true).await.unwrap_err();
    assert_eq!(e.code, ErrorCode::PreconditionFailed);
    assert_eq!(s.purge_queue(&q).await.unwrap(), 2);
    assert_eq!(s.delete_queue(&q, false, true).await.unwrap(), 0);
    s.close().await.unwrap();
}
