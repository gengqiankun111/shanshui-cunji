//! join 模块测试：queryAndJoin（Inner/Left/Right、广播 JOIN、熔断）与写入 Enrich。

use super::merge::should_broadcast;
use super::{
    enrich_check_local, put_with_enrich, query_and_join, JoinBroadcast, JoinSpec, JoinType,
};
use crate::engine::Engine;
use serde_json::{json, Value};

fn engine_with(dir: &std::path::Path) -> Engine {
    Engine::open(dir, &crate::config::Config::default()).unwrap()
}

fn put(engine: &mut Engine, docid: u64, val: Value) {
    let bytes = serde_json::to_vec(&val).unwrap();
    let terms = crate::server::extract_terms(&val);
    let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    engine.put(docid, bytes, &t).unwrap();
}

#[test]
fn inner_join_matches_related_docs() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    // 从表：user 文档
    put(
        &mut e,
        100,
        json!({"docid":100,"type":"user","name":"alice"}),
    );
    put(&mut e, 200, json!({"docid":200,"type":"user","name":"bob"}));
    // 主表：order 文档（user_id 关联）
    put(
        &mut e,
        1,
        json!({"docid":1,"type":"order","user_id":"100","amount":10}),
    );
    put(
        &mut e,
        2,
        json!({"docid":2,"type":"order","user_id":"200","amount":20}),
    );
    put(
        &mut e,
        3,
        json!({"docid":3,"type":"order","user_id":"999","amount":30}),
    ); // 无关联

    let spec = JoinSpec {
        filter: "type=order",
        from_field: "user_id",
        to_field: "docid",
        join_type: JoinType::Inner,
    };
    let rows = query_and_join(&mut e, &spec, 1000, None).unwrap();
    assert_eq!(rows.len(), 2, "Inner 应只保留有关联的行");
    assert_eq!(rows[0].right.as_ref().unwrap()["name"], "alice");
    assert_eq!(rows[1].right.as_ref().unwrap()["name"], "bob");
}

#[test]
fn left_join_keeps_all_left_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    put(
        &mut e,
        100,
        json!({"docid":100,"type":"user","name":"alice"}),
    );
    put(&mut e, 1, json!({"docid":1,"type":"order","user_id":"100"}));
    put(&mut e, 2, json!({"docid":2,"type":"order","user_id":"999"}));

    let spec = JoinSpec {
        filter: "type=order",
        from_field: "user_id",
        to_field: "docid",
        join_type: JoinType::Left,
    };
    let rows = query_and_join(&mut e, &spec, 1000, None).unwrap();
    assert_eq!(rows.len(), 2, "Left 应保留全部主表行");
    assert!(rows[0].right.is_some());
    assert!(rows[1].right.is_none(), "缺失关联的行 right=None");
}

#[test]
fn join_to_field_via_inverted() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    // 从表用非主键字段关联：user 文档的 username 字段
    put(
        &mut e,
        100,
        json!({"docid":100,"type":"user","username":"alice"}),
    );
    put(&mut e, 1, json!({"docid":1,"type":"order","buyer":"alice"}));

    let spec = JoinSpec {
        filter: "type=order",
        from_field: "buyer",
        to_field: "username",
        join_type: JoinType::Inner,
    };
    let rows = query_and_join(&mut e, &spec, 1000, None).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].right.as_ref().unwrap()["docid"], 100);
}

#[test]
fn join_max_rows_fuse() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    for i in 1..=5u64 {
        put(&mut e, i, json!({"docid": i, "type": "order"}));
    }
    let spec = JoinSpec {
        filter: "type=order",
        from_field: "user_id",
        to_field: "docid",
        join_type: JoinType::Left,
    };
    let err = query_and_join(&mut e, &spec, 3, None).unwrap_err();
    assert!(err.to_string().contains("熔断"), "超限应熔断: {err}");
}

#[test]
fn broadcast_enabled_small_table_joins_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    put(
        &mut e,
        100,
        json!({"docid":100,"type":"user","name":"alice"}),
    );
    put(&mut e, 200, json!({"docid":200,"type":"user","name":"bob"}));
    put(
        &mut e,
        1,
        json!({"docid":1,"type":"order","user_id":"100","amount":10}),
    );
    put(
        &mut e,
        2,
        json!({"docid":2,"type":"order","user_id":"999","amount":30}),
    );
    let spec = JoinSpec {
        filter: "type=order",
        from_field: "user_id",
        to_field: "docid",
        join_type: JoinType::Left,
    };
    let bc = JoinBroadcast {
        enabled: true,
        threshold: 100,
    };
    let rows = query_and_join(&mut e, &spec, 1000, Some(bc)).unwrap();
    assert_eq!(rows.len(), 2, "广播 Left 应保留全部主表行");
    assert_eq!(rows[0].right.as_ref().unwrap()["name"], "alice");
    assert!(rows[1].right.is_none(), "无关联行 right=None");
}

#[test]
fn broadcast_first_match_priority_matches_term_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    // 两个 user 共享 username=alice：term 查询取首个文档（docid 小者）
    put(
        &mut e,
        100,
        json!({"docid":100,"type":"user","username":"alice","name":"first"}),
    );
    put(
        &mut e,
        200,
        json!({"docid":200,"type":"user","username":"alice","name":"second"}),
    );
    put(&mut e, 1, json!({"docid":1,"type":"order","buyer":"alice"}));
    let spec = JoinSpec {
        filter: "type=order",
        from_field: "buyer",
        to_field: "username",
        join_type: JoinType::Inner,
    };
    let bc = JoinBroadcast {
        enabled: true,
        threshold: 100,
    };
    let rows = query_and_join(&mut e, &spec, 1000, Some(bc)).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].right.as_ref().unwrap()["name"],
        "first",
        "广播首个命中应取 docid 最小者"
    );
}

#[test]
fn broadcast_falls_back_when_keys_exceed_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    put(
        &mut e,
        100,
        json!({"docid":100,"type":"user","name":"alice"}),
    );
    put(&mut e, 200, json!({"docid":200,"type":"user","name":"bob"}));
    put(&mut e, 1, json!({"docid":1,"type":"order","user_id":"100"}));
    put(&mut e, 2, json!({"docid":2,"type":"order","user_id":"200"}));
    let spec = JoinSpec {
        filter: "type=order",
        from_field: "user_id",
        to_field: "docid",
        join_type: JoinType::Inner,
    };
    // 阈值 1 < 去重 key 数 2 → 回退逐 key 点查，结果仍正确
    let bc = JoinBroadcast {
        enabled: true,
        threshold: 1,
    };
    let rows = query_and_join(&mut e, &spec, 1000, Some(bc)).unwrap();
    assert_eq!(rows.len(), 2, "回退点查结果应一致");
    assert_eq!(rows[0].right.as_ref().unwrap()["name"], "alice");
    assert_eq!(rows[1].right.as_ref().unwrap()["name"], "bob");
}

#[test]
fn broadcast_disabled_keeps_point_query_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    put(
        &mut e,
        100,
        json!({"docid":100,"type":"user","name":"alice"}),
    );
    put(&mut e, 1, json!({"docid":1,"type":"order","user_id":"100"}));
    let spec = JoinSpec {
        filter: "type=order",
        from_field: "user_id",
        to_field: "docid",
        join_type: JoinType::Inner,
    };
    let rows = query_and_join(
        &mut e,
        &spec,
        1000,
        Some(JoinBroadcast {
            enabled: false,
            threshold: 100,
        }),
    )
    .unwrap();
    assert_eq!(rows.len(), 1, "未启用广播应走点查且结果一致");
    assert_eq!(rows[0].right.as_ref().unwrap()["name"], "alice");
}

#[test]
fn should_broadcast_gates_on_enabled_and_threshold() {
    let opt = JoinBroadcast {
        enabled: true,
        threshold: 100,
    };
    assert!(should_broadcast(0, Some(opt)));
    assert!(should_broadcast(100, Some(opt)));
    assert!(!should_broadcast(101, Some(opt)));
    assert!(!should_broadcast(
        1,
        Some(JoinBroadcast {
            enabled: false,
            threshold: 100
        })
    ));
    assert!(!should_broadcast(1, None));
}

#[test]
fn enrich_degrade_writes_original_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    let val = json!({"docid":1,"type":"order","user_id":"999"});
    let bytes = serde_json::to_vec(&val).unwrap();
    let terms = crate::server::extract_terms(&val);
    let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    // 关联缺失 + degrade → 原文档写入成功
    put_with_enrich(&mut e, 1, bytes.clone(), &t, "degrade", |eng, v| {
        enrich_check_local(eng, v, "user_id", "docid")
    })
    .unwrap();
    let got = e.get(1).unwrap().expect("文档应写入");
    assert!(
        String::from_utf8_lossy(&got).contains("user_id"),
        "降级写入原文档"
    );
}

#[test]
fn enrich_reject_denies_write_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    let val = json!({"docid":1,"type":"order","user_id":"999"});
    let bytes = serde_json::to_vec(&val).unwrap();
    let terms = crate::server::extract_terms(&val);
    let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    let err = put_with_enrich(&mut e, 1, bytes, &t, "reject", |eng, v| {
        enrich_check_local(eng, v, "user_id", "docid")
    })
    .unwrap_err();
    assert!(err.to_string().contains("拒绝写入"), "reject 应拒绝: {err}");
    assert!(e.get(1).unwrap().is_none(), "拒绝后不应写入");
}

#[test]
fn enrich_appends_related_doc() {
    let dir = tempfile::tempdir().unwrap();
    let mut e = engine_with(&dir.path());
    put(
        &mut e,
        100,
        json!({"docid":100,"type":"user","name":"alice"}),
    );
    let val = json!({"docid":1,"type":"order","user_id":"100"});
    let bytes = serde_json::to_vec(&val).unwrap();
    let terms = crate::server::extract_terms(&val);
    let t: Vec<&str> = terms.iter().map(|s| s.as_str()).collect();
    put_with_enrich(&mut e, 1, bytes, &t, "reject", |eng, v| {
        enrich_check_local(eng, v, "user_id", "docid")
    })
    .unwrap();
    let got = e.get(1).unwrap().expect("文档应写入");
    let got_val: Value = serde_json::from_slice(&got).unwrap();
    assert_eq!(
        got_val["_enrich"]["related"]["name"], "alice",
        "应展开关联文档"
    );
}
