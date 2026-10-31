//! Integration tests against a live broker (see scripts/test-broker.sh).

use rabbitmq_core::ErrorCode;
use rabbitmq_management::*;
use rabbitmq_testkit::{broker, unique, Admin};
use serde_json::{json, Map};

#[tokio::test]
async fn overview_nodes_health() {
    let Some(p) = broker().await else { return };
    let m = ManagementClient::from_profile(&p).unwrap();
    let o = m.overview().await.unwrap();
    assert!(o.rabbitmq_version.starts_with('4'), "{}", o.rabbitmq_version);
    let nodes = m.nodes().await.unwrap();
    assert!(!nodes.is_empty() && nodes[0].running);
    let h = m.health().await.unwrap();
    assert_eq!(h["alarms"]["ok"], true);
    assert!(!m.feature_flags().await.unwrap().is_empty());
    assert_eq!(m.whoami().await.unwrap()["name"], "guest");
}

#[tokio::test]
async fn queue_exchange_binding_lifecycle_and_topology() {
    let Some(p) = broker().await else { return };
    let m = ManagementClient::from_profile(&p).unwrap();
    let q = unique("mq");
    let x = unique("mx");
    m.declare_exchange("/", &x, &json!({"type": "topic", "durable": true})).await.unwrap();
    m.declare_queue("/", &q, &json!({"durable": true, "arguments": {"x-queue-type": "quorum"}})).await.unwrap();
    m.create_binding("/", &x, "queue", &q, "orders.#", &Map::new()).await.unwrap();

    let info = m.queue("/", &q).await.unwrap();
    assert_eq!(info.queue_type(), "quorum");
    let bs = m.bindings_from("/", &x).await.unwrap();
    assert_eq!(bs.len(), 1);
    assert_eq!(bs[0].routing_key, "orders.#");

    let topo = m.topology("/").await.unwrap();
    let pred = topo.simulate(&x, "orders.eu", &Map::new());
    assert_eq!(pred.queues, vec![q.clone()]);
    assert!(topo.simulate(&x, "invoices", &Map::new()).unroutable);

    // Re-declaring with different properties must surface precondition_failed,
    // never silently change the existing definition.
    let e = m
        .declare_exchange("/", &x, &json!({"type": "fanout", "durable": true}))
        .await
        .unwrap_err();
    assert_eq!(e.code, ErrorCode::PreconditionFailed, "{e:?}");
    assert_eq!(m.exchange("/", &x).await.unwrap().kind, "topic");

    let key = bs[0].properties_key.clone().unwrap();
    m.delete_binding("/", &x, "queue", &q, &key).await.unwrap();
    assert!(m.bindings_from("/", &x).await.unwrap().is_empty());
    m.delete_queue("/", &q, false, false).await.unwrap();
    m.delete_exchange("/", &x, false).await.unwrap();
    assert_eq!(m.queue("/", &q).await.unwrap_err().code, ErrorCode::NotFound);
}

#[tokio::test]
async fn paging_and_filtering() {
    let Some(p) = broker().await else { return };
    let m = ManagementClient::from_profile(&p).unwrap();
    let prefix = unique("page");
    for i in 0..3 {
        m.declare_queue("/", &format!("{prefix}.{i}"), &json!({"durable": true})).await.unwrap();
    }
    let page = m
        .queues(
            Some("/"),
            &ListQuery {
                page: Some(1),
                page_size: Some(2),
                name: Some(format!("^{}", prefix.replace('.', "\\."))),
                use_regex: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let page: Page<QueueInfo> = serde_json::from_value(page).unwrap();
    assert_eq!(page.filtered_count, 3);
    assert_eq!(page.items.len(), 2);
    for i in 0..3 {
        m.delete_queue("/", &format!("{prefix}.{i}"), false, false).await.unwrap();
    }
}

#[tokio::test]
async fn definitions_export_and_diff() {
    let Some(p) = broker().await else { return };
    let m = ManagementClient::from_profile(&p).unwrap();
    let q = unique("defs");
    m.declare_queue("/", &q, &json!({"durable": true})).await.unwrap();
    let defs = m.export_definitions(Some("/")).await.unwrap();
    let from_defs = topology_from_definitions(&defs, None);
    assert!(from_defs.queues.iter().any(|x| x.name == q));
    m.delete_queue("/", &q, false, false).await.unwrap();
    let live = m.topology("/").await.unwrap();
    let d = rabbitmq_core::topology::diff(&from_defs, &live);
    assert!(d.removed.contains(&format!("queue {q}")), "{d:?}");
}

#[tokio::test]
async fn users_permissions_and_access_refused() {
    let Some(p) = broker().await else { return };
    let m = ManagementClient::from_profile(&p).unwrap();
    let user = unique("user");
    m.put_user(&user, Some("pw-123"), "monitoring").await.unwrap();
    m.set_permissions("/", &user, "^$", "^$", ".*").await.unwrap();
    let perms = m.user_permissions(&user).await.unwrap();
    assert_eq!(perms[0].configure, "^$");
    let users = serde_json::to_value(m.users().await.unwrap()).unwrap();
    assert!(users.as_array().unwrap().iter().all(|u| u.get("password_hash").is_none()));

    // The restricted user can read but not declare.
    let mut rp = p.clone();
    rp.username = user.clone();
    rp.password = rabbitmq_core::profile::Secret::new("pw-123");
    let rm = ManagementClient::from_profile(&rp).unwrap();
    rm.overview().await.unwrap();
    let e = rm.declare_queue("/", &unique("denied"), &json!({"durable": true})).await.unwrap_err();
    assert!(matches!(e.code, ErrorCode::AccessRefused | ErrorCode::AuthenticationFailed), "{e:?}");

    // Wrong password -> authentication_failed, and no secret in the message.
    rp.password = rabbitmq_core::profile::Secret::new("wrong-secret");
    let bad = ManagementClient::from_profile(&rp).unwrap();
    let e = bad.overview().await.unwrap_err();
    assert_eq!(e.code, ErrorCode::AuthenticationFailed);
    assert!(!e.message.contains("wrong-secret"));

    m.delete_user(&user).await.unwrap();
}

#[tokio::test]
async fn policies_roundtrip() {
    let Some(p) = broker().await else { return };
    let m = ManagementClient::from_profile(&p).unwrap();
    let name = unique("pol");
    let pol = Policy {
        name: name.clone(),
        vhost: "/".into(),
        pattern: format!("^{}$", name.replace('.', "\\.")),
        apply_to: "queues".into(),
        definition: json!({"max-length": 10}).as_object().unwrap().clone(),
        priority: 1,
    };
    m.put_policy(&pol).await.unwrap();
    assert!(m.policies(Some("/")).await.unwrap().iter().any(|x| x.name == name));
    m.delete_policy("/", &name).await.unwrap();
    let admin = Admin::new(&p);
    assert!(admin.get("policies/%2F").await.as_array().unwrap().iter().all(|x| x["name"] != name.as_str()));
}
