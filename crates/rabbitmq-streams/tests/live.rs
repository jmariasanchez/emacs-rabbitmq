//! Live stream tests (requires the rabbitmq_stream plugin on :5552).

use rabbitmq_core::events::MemorySink;
use rabbitmq_core::message::*;
use rabbitmq_core::ErrorCode;
use rabbitmq_streams::*;
use rabbitmq_testkit::{broker, unique, Admin};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

async fn session() -> Option<(Arc<StreamSession>, Arc<MemorySink>, Admin)> {
    let p = broker().await?;
    let sink = MemorySink::new();
    let s = StreamSession::connect(unique("ss"), p.clone(), sink.clone())
        .await
        .expect("stream connect");
    Some((s, sink, Admin::new(&p)))
}

fn sm(body: &str) -> StreamMessage {
    serde_json::from_value(json!({"payload": {"data": body}})).unwrap()
}

#[tokio::test]
async fn replay_is_non_destructive_and_repeatable() {
    let Some((s, _, admin)) = session().await else {
        return;
    };
    let st = unique("st");
    s.create(&CreateStream {
        name: st.clone(),
        max_age: Some("1D".into()),
        max_length_bytes: Some(10_000_000),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut msgs: Vec<StreamMessage> = (0..10).map(|i| sm(&format!("m{i}"))).collect();
    msgs[0].properties.message_id = Some("first".into());
    msgs[0].properties.headers.insert("k".into(), json!("v"));
    let out = s
        .publish(&StreamPublish {
            stream: st.clone(),
            messages: msgs,
            producer_name: None,
            timeout_ms: 10_000,
        })
        .await
        .unwrap();
    assert!(
        out.iter().all(|o| o.status == PublishStatus::Confirmed),
        "{out:?}"
    );

    let req = |start: StartAt, max: usize| ReadRequest {
        stream: st.clone(),
        start,
        max,
        idle_ms: 700,
        filter: None,
        match_unfiltered: false,
    };
    let a = s.read(&req(StartAt::First, 100)).await.unwrap();
    let b = s.read(&req(StartAt::First, 100)).await.unwrap();
    assert!(!a.destructive);
    assert_eq!(a.messages.len(), 10);
    let bodies = |r: &ReadResult| {
        r.messages
            .iter()
            .map(|m| m.payload.data.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(bodies(&a), bodies(&b), "replay must be repeatable");
    assert_eq!(
        a.messages[0].properties.message_id.as_deref(),
        Some("first")
    );
    assert_eq!(a.messages[0].properties.headers["k"], "v");
    assert!(matches!(
        a.messages[0].source,
        DeliverySource::Stream { .. }
    ));

    // Offset-based and bounded reads.
    let third = a.first_offset.unwrap() + 3;
    let c = s
        .read(&req(StartAt::Offset { offset: third }, 2))
        .await
        .unwrap();
    assert_eq!(bodies(&c), vec!["m3", "m4"]);
    assert!(c.more);

    // Timestamp in the future returns nothing; in the past returns everything.
    let fut = s
        .read(&req(
            StartAt::Timestamp {
                timestamp: (now_millis() + 3_600_000) as i64,
            },
            100,
        ))
        .await
        .unwrap();
    assert!(fut.messages.is_empty());
    let past = s
        .read(&req(StartAt::Timestamp { timestamp: 0 }, 100))
        .await
        .unwrap();
    assert_eq!(past.messages.len(), 10);

    // The stream still reports all 10 messages: nothing was consumed.
    let q = admin
        .wait_queue(&st, |v| v["messages"].as_u64() == Some(10))
        .await;
    assert_eq!(q["type"], "stream");

    let md = s.metadata(&[st.clone()]).await.unwrap();
    assert_eq!(md[&st]["ok"], true);
    s.delete(&st, false).await.unwrap();
    assert_eq!(
        s.read(&req(StartAt::First, 1)).await.unwrap_err().code,
        ErrorCode::NotFound
    );
    s.close().await.unwrap();
}

#[tokio::test]
async fn named_producer_deduplicates() {
    let Some((s, _, _)) = session().await else {
        return;
    };
    let st = unique("dedup");
    s.create(&CreateStream {
        name: st.clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    let producer = unique("prod");
    let mk = |ids: &[u64]| StreamPublish {
        stream: st.clone(),
        messages: ids
            .iter()
            .map(|i| {
                let mut m = sm(&format!("p{i}"));
                m.publishing_id = Some(*i);
                m
            })
            .collect(),
        producer_name: Some(producer.clone()),
        timeout_ms: 10_000,
    };
    s.publish(&mk(&[1, 2, 3])).await.unwrap();
    assert_eq!(s.producer_sequence(&producer, &st).await.unwrap(), 3);
    // Re-sending ids 2 and 3 (e.g. after an uncertain publish) must not duplicate.
    s.publish(&mk(&[2, 3, 4])).await.unwrap();
    let r = s
        .read(&ReadRequest {
            stream: st.clone(),
            start: StartAt::First,
            max: 100,
            idle_ms: 700,
            filter: None,
            match_unfiltered: false,
        })
        .await
        .unwrap();
    let bodies: Vec<_> = r.messages.iter().map(|m| m.payload.data.as_str()).collect();
    assert_eq!(bodies, vec!["p1", "p2", "p3", "p4"]);
    s.delete(&st, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn filtering_and_offset_tracking() {
    let Some((s, _, _)) = session().await else {
        return;
    };
    let st = unique("filt");
    s.create(&CreateStream {
        name: st.clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    let msgs: Vec<StreamMessage> = ["eu", "us", "eu", "apac"]
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut m = sm(&format!("{r}{i}"));
            m.filter_value = Some(r.to_string());
            m
        })
        .collect();
    s.publish(&StreamPublish {
        stream: st.clone(),
        messages: msgs,
        producer_name: None,
        timeout_ms: 10_000,
    })
    .await
    .unwrap();
    let r = s
        .read(&ReadRequest {
            stream: st.clone(),
            start: StartAt::First,
            max: 100,
            idle_ms: 700,
            filter: Some(vec!["eu".into()]),
            match_unfiltered: false,
        })
        .await
        .unwrap();
    let bodies: Vec<_> = r.messages.iter().map(|m| m.payload.data.as_str()).collect();
    assert_eq!(bodies, vec!["eu0", "eu2"]);

    let consumer = unique("tracker");
    assert_eq!(s.query_offset(&consumer, &st).await.unwrap(), None);
    s.store_offset(&consumer, &st, 2).await.unwrap();
    assert_eq!(s.query_offset(&consumer, &st).await.unwrap(), Some(2));
    s.delete(&st, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn live_subscription_delivers_batches() {
    let Some((s, sink, _)) = session().await else {
        return;
    };
    let st = unique("live");
    s.create(&CreateStream {
        name: st.clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    let sub = s
        .subscribe(
            serde_json::from_value(
                json!({"stream": st, "start": {"type": "next"}, "flush_ms": 20}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    s.publish(&StreamPublish {
        stream: st.clone(),
        messages: (0..5).map(|i| sm(&format!("l{i}"))).collect(),
        producer_name: None,
        timeout_ms: 10_000,
    })
    .await
    .unwrap();
    let mut got = 0;
    for _ in 0..50 {
        got = sink
            .snapshot()
            .iter()
            .filter(|(m, p)| m == "$/deliveries" && p["consumer"] == sub.id.as_str())
            .map(|(_, p)| p["messages"].as_array().map(|a| a.len()).unwrap_or(0))
            .sum::<usize>();
        if got >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(got, 5);
    let info = s.unsubscribe(&sub.id).await.unwrap();
    assert_eq!(info.delivered, 5);
    // SAC without a name is rejected.
    let e = s
        .subscribe(
            serde_json::from_value(
                json!({"stream": st, "start": {"type": "first"}, "single_active_consumer": true}),
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::InvalidParams);
    s.delete(&st, false).await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test]
async fn super_stream_partitions_and_routes() {
    let Some((s, _, _)) = session().await else {
        return;
    };
    let ss = unique("super");
    s.create(&CreateStream {
        name: ss.clone(),
        partitions: Some(3),
        ..Default::default()
    })
    .await
    .unwrap();
    let parts = s.partitions(&ss).await.unwrap();
    assert_eq!(parts.len(), 3);
    assert!(parts.iter().all(|p| p.starts_with(&ss)));
    let r = s.route(&ss, "1").await.unwrap();
    assert_eq!(r, vec![format!("{ss}-1")]);
    s.delete(&ss, true).await.unwrap();
    assert!(s.partitions(&ss).await.is_err());
    s.close().await.unwrap();
}
