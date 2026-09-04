//! SAGA 网关（Ex-2.5 + 13.6 + 13.7）：`/saga/start` `/saga/status` `/saga/compensate`
//! 端点 + 后台对账线程（spawn_reconciler）+ 步骤 JSON 重建（http_steps_from_json）。
//!
//! 参照 design_extension 13.1：协调器状态持久化 `{data_dir}/saga/saga-{tx_id}.json`，
//! 崩溃恢复续跑；业务步骤为 HTTP 端点（HttpStep），非 2xx/超时 → 失败逆序补偿。

use std::collections::HashMap;

use serde_json::{json, Value};

use super::json::parse_query;
use super::{
    now_ms, SagaShared, SagaStepsCache, SAGA_MAX_BACKOFF_MS, SAGA_RECONCILE_INTERVAL_SECS,
    SAGA_STALL_MS,
};

/// 13.7 后台对账线程：周期扫描未终态事务，按指数退避自动续补偿（无步骤定义则跳过留人工）。
pub(crate) fn spawn_reconciler(saga: SagaShared, steps_cache: SagaStepsCache) {
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(SAGA_RECONCILE_INTERVAL_SECS));
        let now = now_ms();
        let (mut coord, cache) = match (saga.lock(), steps_cache.lock()) {
            (Ok(c), Ok(s)) => (c, s),
            _ => continue, // 中毒/其他：下周期再试
        };
        let retried = coord.retry_pending(
            |tx| {
                cache
                    .get(tx)
                    .map(http_steps_from_json)
                    .unwrap_or_default()
            },
            now,
            SAGA_STALL_MS,
            SAGA_MAX_BACKOFF_MS,
        );
        if retried > 0 {
            tracing::info!("SAGA 对账器触发 {retried} 个事务续补偿");
        }
    });
}

/// 从 steps JSON 数组重建 HTTP 步骤（对账重试与 start 解析共用；非法项跳过）。
fn http_steps_from_json(arr: &Value) -> Vec<Box<dyn crate::saga::SagaStep>> {
    let mut out: Vec<Box<dyn crate::saga::SagaStep>> = Vec::new();
    if let Some(items) = arr.as_array() {
        for st in items {
            if let (Some(name), Some(compensate_url)) = (
                st.get("name").and_then(|x| x.as_str()),
                st.get("compensate_url").and_then(|x| x.as_str()),
            ) {
                let action_url = st.get("action_url").and_then(|x| x.as_str()).unwrap_or("");
                let payload = st
                    .get("payload")
                    .map(|p| serde_json::to_string(p).unwrap_or_default())
                    .unwrap_or_default()
                    .into_bytes();
                out.push(Box::new(crate::saga::HttpStep::new(
                    name, action_url, compensate_url, payload,
                )));
            }
        }
    }
    out
}

/// `POST /saga/start` `{"tx_id":"t1","steps":[{"name":"扣款","action_url":"...",
/// "compensate_url":"...","payload":"...","depends_on":["..."]}]}` → 执行（失败自动逆序补偿）。
/// 13.6：steps[i] 可选 `depends_on`（依赖步骤名数组）→ 拓扑并行执行；无依赖 → 原串行 `run`。
/// 13.7：成功后缓存步骤定义（对账重试重建用）。
pub(crate) fn handle_saga_start(
    saga: Option<&SagaShared>,
    steps_cache: Option<&SagaStepsCache>,
    body: &[u8],
) -> (u16, String) {
    let Some(coord_arc) = saga else {
        return (501, json!({"error": "SAGA 协调器未挂载"}).to_string());
    };
    let v: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return (400, json!({"error": format!("请求体解析失败: {e}")}).to_string()),
    };
    let Some(tx_id) = v.get("tx_id").and_then(|x| x.as_str()) else {
        return (400, json!({"error": "缺少 tx_id"}).to_string());
    };
    let Some(steps_arr) = v.get("steps").and_then(|x| x.as_array()) else {
        return (400, json!({"error": "缺少 steps 数组"}).to_string());
    };
    // 解析步骤：name → 索引（depends_on 引用解析）
    let mut name_idx: HashMap<&str, usize> = HashMap::new();
    for (i, st) in steps_arr.iter().enumerate() {
        let Some(name) = st.get("name").and_then(|x| x.as_str()) else {
            return (400, json!({"error": format!("steps[{i}] 缺少 name")}).to_string());
        };
        name_idx.insert(name, i);
    }
    let mut steps: Vec<Box<dyn crate::saga::SagaStep>> = Vec::new();
    let mut deps: Vec<Vec<usize>> = Vec::new();
    for (i, st) in steps_arr.iter().enumerate() {
        let (Some(name), Some(action_url), Some(compensate_url)) = (
            st.get("name").and_then(|x| x.as_str()),
            st.get("action_url").and_then(|x| x.as_str()),
            st.get("compensate_url").and_then(|x| x.as_str()),
        ) else {
            return (
                400,
                json!({"error": format!("steps[{i}] 缺少 name/action_url/compensate_url")}).to_string(),
            );
        };
        let payload = st
            .get("payload")
            .map(|p| serde_json::to_string(p).unwrap_or_default())
            .unwrap_or_default()
            .into_bytes();
        // 13.6 depends_on：依赖步骤名数组 → 索引（未知名/自依赖 → 400）
        let mut di = Vec::new();
        if let Some(dep) = st.get("depends_on").and_then(|x| x.as_array()) {
            for d in dep {
                let Some(dn) = d.as_str() else {
                    return (400, json!({"error": format!("steps[{i}].depends_on 项须为字符串")}).to_string());
                };
                match name_idx.get(dn) {
                    Some(&idx) => di.push(idx),
                    None => {
                        return (
                            400,
                            json!({"error": format!("steps[{i}].depends_on 引用未知步骤: {dn}")}).to_string(),
                        )
                    }
                }
            }
        }
        deps.push(di);
        steps.push(Box::new(crate::saga::HttpStep::new(name, action_url, compensate_url, payload)));
    }
    let refs: Vec<&dyn crate::saga::SagaStep> = steps.iter().map(|s| s.as_ref()).collect();
    // 13.7：缓存步骤定义（对账重试重建；失败仅告警不阻断）
    if let Some(cache) = steps_cache {
        if let Ok(mut c) = cache.lock() {
            c.insert(tx_id.to_string(), Value::Array(steps_arr.clone()));
        }
    }
    let outcome = (|| -> crate::error::Result<crate::saga::SagaStatus> {
        let mut coord = coord_arc.lock().unwrap();
        match coord.status(tx_id) {
            Some(st) if st.status.is_terminal() => return Ok(st.status), // 终态幂等
            Some(_) => {} // 已登记：run 续跑（含崩溃恢复）
            None => {
                coord.start(tx_id)?;
            }
        }
        // 13.6：有依赖声明 → 拓扑并行；否则原串行 run（兼容旧请求）
        let has_deps = deps.iter().any(|d| !d.is_empty());
        if has_deps {
            // 提前环/非法依赖校验 → 400（run_parallel 内部同样校验，双保险）
            if let Err(e) = crate::saga::topo_layers(steps.len(), &deps) {
                return Err(e);
            }
        }
        if has_deps {
            coord.run_parallel(tx_id, &refs, &deps)
        } else {
            coord.run(tx_id, &refs)
        }
    })();
    match outcome {
        Ok(status) => {
            let st = coord_arc.lock().unwrap().status(tx_id).unwrap().clone();
            (200, json!({"tx_id": tx_id, "status": status, "executed_steps": st.executed_steps, "last_error": st.last_error}).to_string())
        }
        Err(e) => (400, json!({"error": e.to_string()}).to_string()),
    }
}

/// `GET /saga/status?tx_id=` → transactionId → status 回查（屏障接口依据）。
pub(crate) fn handle_saga_status(saga: Option<&SagaShared>, query: &str) -> (u16, String) {
    let Some(coord) = saga else {
        return (501, json!({"error": "SAGA 协调器未挂载"}).to_string());
    };
    let params = parse_query(query);
    let Some(tx_id) = params.iter().find(|(k, _)| k == "tx_id").map(|(_, v)| v.clone()) else {
        return (400, json!({"error": "缺少 tx_id 参数"}).to_string());
    };
    let st = coord.lock().unwrap().status(&tx_id).cloned();
    match st {
        Some(st) => (200, json!({"tx_id": tx_id, "status": st.status, "executed_steps": st.executed_steps, "compensated_steps": st.compensated_steps, "last_error": st.last_error, "retry_count": st.retry_count}).to_string()),
        None => (404, json!({"error": format!("SAGA 事务不存在: {tx_id}")}).to_string()),
    }
}

/// `POST /saga/compensate` `{"tx_id":"t1"}` → 强制对已登记分支逆序补偿（重试/人工干预）。
/// 步骤定义从持久化状态无从恢复，故请求可带可选 `steps`（缺省按已登记分支续补偿）。
pub(crate) fn handle_saga_compensate(saga: Option<&SagaShared>, body: &[u8]) -> (u16, String) {
    let Some(coord) = saga else {
        return (501, json!({"error": "SAGA 协调器未挂载"}).to_string());
    };
    let v: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return (400, json!({"error": format!("请求体解析失败: {e}")}).to_string()),
    };
    let Some(tx_id) = v.get("tx_id").and_then(|x| x.as_str()) else {
        return (400, json!({"error": "缺少 tx_id"}).to_string());
    };
    // 可选 steps（缺省空：仅把持久化状态置 Compensating；后续 run 续补偿）
    let steps = v.get("steps").map(http_steps_from_json).unwrap_or_default();
    let refs: Vec<&dyn crate::saga::SagaStep> = steps.iter().map(|s| s.as_ref()).collect();
    let mut coord = coord.lock().unwrap();
    if steps.is_empty() {
        // 无步骤定义：无法发起网络补偿，返回当前状态（续跑依赖 /saga/start 带步骤）
        return match coord.status(tx_id) {
            Some(st) => (200, json!({"tx_id": tx_id, "status": st.status, "note": "无步骤定义，仅置待补偿（可用 /saga/start 携带步骤续跑）"}).to_string()),
            None => (404, json!({"error": format!("SAGA 事务不存在: {tx_id}")}).to_string()),
        };
    }
    match coord.compensate(tx_id, &refs) {
        Ok(status) => (200, json!({"tx_id": tx_id, "status": status}).to_string()),
        Err(e) => (500, json!({"error": e.to_string()}).to_string()),
    }
}
