//! Live AMQP 1.0 tests against RabbitMQ 4.x (native AMQP 1.0 on port 5672).

use rabbitmq_amqp10::*;
use rabbitmq_core::events::{names, MemorySink};
use rabbitmq_core::message::*;
use rabbitmq_testkit::{broker, pct, unique, Admin};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

async fn setup() -> Option<(Arc<Amqp10Session>, Arc<MemorySink>, Admin)> {
    let p = broker().await?;
    let sink = MemorySink::new();
    let s = Amqp10Session::connect(unique("a10"), p.clone(), sink.clone())
        .await
        .expect("amqp10 connect");
    Some((s, sink, Admin::new(&p)))
}

fn msgs(n: usize) -> Vec<(Payload, MessageProperties)> {
    (0..n)
        .map(|i| {
            let mut p = MessageProperties::default();
            p.message_id = Some(format!("id-{i}"));
            p.headers.insert("i".into(), json!(i));
            (Payload::text(format!("amqp10-{i}")), p)
        })
        .collect()
}

async fn wait_deliveries(sink: &MemorySink, link: &str, n: usize) -> Vec<InboundMessage> {
    for _ in 0..60 {
        let got: Vec<InboundMessage> = sink
            .snapshot()
            .into_iter()
            .filter(|(m, p)| m == names::DELIVERIES && p["consumer"] == link)
            .flat_map(|(_, p)| {
                serde_json::from_value::<Vec<InboundMessage>>(p["messages"].clone()).unwrap()
            })
            .collect();
        if got.len() >= n {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("did not receive {n} deliveries on {link}");
}

fn did(m: &InboundMessage) -> u32 {
    match &m.source {
        DeliverySource::Amqp10 { delivery_id, .. } => *delivery_id,
        other => panic!("unexpected source {other:?}"),
    }
}

#[tokio::test]
async fn send_receive_settle_outcomes_and_credit() {
    let Some((s, sink, admin)) = setup().await else {
        return;
    };
    let q = unique("a10q");
    assert_eq!(
        admin
            .put(
                &format!("queues/%2F/{}", pct(&q)),
                json!({"durable": true, "arguments": {"x-queue-type": "quorum"}})
            )
            .await,
        201
    );

    let snd = s
        .open_sender(&Address::Queue { queue: q.clone() })
        .await
        .unwrap();
    let out = s
        .send(&snd.id, &msgs(6), Duration::from_secs(10))
        .await
        .unwrap();
    assert!(
        out.iter().all(|o| o.status == PublishStatus::Confirmed),
        "{out:?}"
    );
    admin.wait_queue(&q, |v| v["messages"] == 6).await;

    // Manual credit of 2: the broker must stop after two deliveries.
    let rcv = s
        .open_receiver(serde_json::from_value(json!({"address": {"type": "queue", "queue": q}, "credit": 2, "auto_credit": false, "flush_ms": 20})).unwrap())
        .await
        .unwrap();
    let first = wait_deliveries(&sink, &rcv.id, 2).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        wait_deliveries(&sink, &rcv.id, 2).await.len(),
        2,
        "credit must bound deliveries"
    );
    assert_eq!(first[0].payload.data, "amqp10-0");
    assert_eq!(first[0].properties.message_id.as_deref(), Some("id-0"));
    assert_eq!(first[0].properties.headers["i"], 0);

    // accepted removes; released requeues.
    s.settle(&rcv.id, did(&first[0]), Settlement::Ack)
        .await
        .unwrap();
    s.settle(&rcv.id, did(&first[1]), Settlement::Release)
        .await
        .unwrap();
    // Settling twice is rejected.
    assert!(s
        .settle(&rcv.id, did(&first[0]), Settlement::Ack)
        .await
        .is_err());

    s.set_credit(&rcv.id, 10).await.unwrap();
    let all = wait_deliveries(&sink, &rcv.id, 7).await;
    // The released message comes back (redelivered); 5 remaining + 2 initial = 7.
    let ids: Vec<u32> = all.iter().skip(2).map(did).collect();
    for id in ids {
        s.settle(&rcv.id, id, Settlement::Ack).await.unwrap();
    }
    admin.wait_queue(&q, |v| v["messages"] == 0).await;

    s.close_receiver(&rcv.id).await.unwrap();
    s.close_sender(&snd.id).await.unwrap();
    admin.delete_queue(&q).await;
    s.close().await.unwrap();
}

#[tokio::test]
async fn unroutable_exchange_publish_is_released_not_accepted() {
    let Some((s, _, admin)) = setup().await else {
        return;
    };
    let x = unique("a10x");
    assert_eq!(
        admin
            .put(
                &format!("exchanges/%2F/{}", pct(&x)),
                json!({"type": "direct", "durable": true})
            )
            .await,
        201
    );
    let snd = s
        .open_sender(&Address::Exchange {
            exchange: x.clone(),
            routing_key: Some("nobody".into()),
        })
        .await
        .unwrap();
    let out = s
        .send(&snd.id, &msgs(1), Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(out[0].status, PublishStatus::Returned, "{out:?}");
    s.close_sender(&snd.id).await.unwrap();
    admin.delete_exchange(&x).await;
    s.close().await.unwrap();
}

#[tokio::test]
async fn attaching_to_missing_queue_fails_cleanly() {
    let Some((s, _, _)) = setup().await else {
        return;
    };
    let e = s
        .open_sender(&Address::Queue {
            queue: unique("missing"),
        })
        .await
        .unwrap_err();
    assert!(!e.message.is_empty());
    let e = s
        .open_receiver(
            serde_json::from_value(
                json!({"address": {"type": "queue", "queue": unique("missing")}}),
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(!e.message.is_empty());
    assert!(s.links().await.is_empty());
    s.close().await.unwrap();
}

#[tokio::test]
async fn reject_dead_letters() {
    let Some((s, sink, admin)) = setup().await else {
        return;
    };
    let (q, dlq, dlx) = (unique("a10src"), unique("a10dlq"), unique("a10dlx"));
    admin
        .put(
            &format!("exchanges/%2F/{}", pct(&dlx)),
            json!({"type": "fanout", "durable": true}),
        )
        .await;
    admin
        .put(
            &format!("queues/%2F/{}", pct(&dlq)),
            json!({"durable": true}),
        )
        .await;
    assert_eq!(
        admin
            .post(
                &format!("bindings/%2F/e/{}/q/{}", pct(&dlx), pct(&dlq)),
                json!({"routing_key": ""})
            )
            .await,
        201
    );
    admin
        .put(&format!("queues/%2F/{}", pct(&q)), json!({"durable": true, "arguments": {"x-queue-type": "quorum", "x-dead-letter-exchange": dlx}}))
        .await;
    let snd = s
        .open_sender(&Address::Queue { queue: q.clone() })
        .await
        .unwrap();
    s.send(&snd.id, &msgs(1), Duration::from_secs(10))
        .await
        .unwrap();
    let rcv = s
        .open_receiver(
            serde_json::from_value(
                json!({"address": {"type": "queue", "queue": q}, "flush_ms": 20}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let d = wait_deliveries(&sink, &rcv.id, 1).await;
    s.settle(&rcv.id, did(&d[0]), Settlement::Reject { requeue: false })
        .await
        .unwrap();
    admin.wait_queue(&dlq, |v| v["messages"] == 1).await;
    s.close().await.unwrap();
    for n in [&q, &dlq] {
        admin.delete_queue(n).await;
    }
    admin.delete_exchange(&dlx).await;
}
