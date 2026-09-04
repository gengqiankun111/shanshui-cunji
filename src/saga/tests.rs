//! SAGA 引擎单元测试（由原 saga.rs `#[cfg(test)] mod tests` 迁移，测试代码零改动）。
//!
//! 覆盖：正向成功/中段失败逆序补偿、崩溃恢复续跑/续补偿、悬挂/空回滚屏障、
//! 缺步骤定义保持 Compensating（13.5 修复变体）、HTTP 超时屏障空转（13.5.2）、
//! 拓扑并行（topo_layers + run_parallel，13.6）、对账器（retry_pending，13.7）。

use super::*;
use crate::error::{Error, Result};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// 副作用记录（多测试并行互斥共享计数）。
static TEST_LOCK: Mutex<()> = Mutex::new(());
static FWD_CALLS: AtomicUsize = AtomicUsize::new(0);
static CMP_CALLS: AtomicUsize = AtomicUsize::new(0);

/// 简单步骤：正向/补偿都成功并计数。
struct SimpleStep {
    name: &'static str,
    fail_forward: bool,
}

impl SagaStep for SimpleStep {
    fn name(&self) -> &str {
        self.name
    }
    fn forward(&self) -> Result<()> {
        FWD_CALLS.fetch_add(1, Ordering::SeqCst);
        if self.fail_forward {
            return Err(Error::Config(format!("{} 业务失败", self.name)));
        }
        Ok(())
    }
    fn compensate(&self) -> Result<()> {
        CMP_CALLS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn steps() -> Vec<Box<dyn SagaStep>> {
    vec![
        Box::new(SimpleStep { name: "扣款", fail_forward: false }),
        Box::new(SimpleStep { name: "发货", fail_forward: false }),
    ]
}

fn refs(steps: &[Box<dyn SagaStep>]) -> Vec<&dyn SagaStep> {
    steps.iter().map(|s| s.as_ref()).collect()
}

#[test]
fn forward_all_success_no_compensate() {
    let _g = TEST_LOCK.lock().unwrap();
    FWD_CALLS.store(0, Ordering::SeqCst);
    CMP_CALLS.store(0, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tx1").unwrap();
    let steps = steps();
    let s = c.run("tx1", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Succeeded);
    assert_eq!(c.status("tx1").unwrap().executed_steps.len(), 2);
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 0, "成功路径无补偿");
}

#[test]
fn mid_failure_reverse_compensate() {
    let _g = TEST_LOCK.lock().unwrap();
    FWD_CALLS.store(0, Ordering::SeqCst);
    CMP_CALLS.store(0, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tx2").unwrap();
    let steps: Vec<Box<dyn SagaStep>> = vec![
        Box::new(SimpleStep { name: "a", fail_forward: false }),
        Box::new(SimpleStep { name: "b", fail_forward: true }),
        Box::new(SimpleStep { name: "c", fail_forward: false }),
    ];
    let s = c.run("tx2", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensated, "中段失败 → 补偿完成");
    let st = c.status("tx2").unwrap();
    assert_eq!(st.executed_steps, vec!["a"], "仅已登记分支待补偿");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 1, "只补偿 a");
    assert_eq!(FWD_CALLS.load(Ordering::SeqCst), 2, "a 成功 + b 失败尝试一次");
}

#[test]
fn state_survives_reopen_and_resumes() {
    let _g = TEST_LOCK.lock().unwrap();
    FWD_CALLS.store(0, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    {
        let mut c = SagaCoordinator::open(dir.path()).unwrap();
        c.start("tx3").unwrap();
        let steps = steps();
        c.run("tx3", &refs(&steps)).unwrap();
    } // 协调器丢弃 = 崩溃
    let c2 = SagaCoordinator::open(dir.path()).unwrap();
    let st = c2.status("tx3").unwrap();
    assert_eq!(st.status, SagaStatus::Succeeded, "重启恢复终态");
    assert_eq!(st.executed_steps.len(), 2);
}

#[test]
fn terminal_rejects_late_forward() {
    let _g = TEST_LOCK.lock().unwrap();
    FWD_CALLS.store(0, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tx4").unwrap();
    let steps = steps();
    c.run("tx4", &refs(&steps)).unwrap();
    // 悬挂防护：终态后重放 run → 不重复执行
    c.run("tx4", &refs(&steps)).unwrap();
    assert_eq!(FWD_CALLS.load(Ordering::SeqCst), 2, "终态拒绝重复正向");
}

#[test]
fn compensate_retry_then_success() {
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tx5").unwrap();
    let attempts = std::sync::Arc::new(AtomicUsize::new(0));
    let steps: Vec<Box<dyn SagaStep>> = vec![
        Box::new(ClosureStep::new("refund", || Ok(()), {
            let n = attempts.clone();
            move || {
                if n.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err(Error::Rpc("补偿服务暂不可用".into()));
                }
                Ok(())
            }
        })),
        Box::new(SimpleStep { name: "ship", fail_forward: true }),
    ];
    let s = c.run("tx5", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensating, "首次补偿失败保持 Compensating");
    let s = c.run("tx5", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensating, "二次补偿仍失败");
    let s = c.run("tx5", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensated, "重试后补偿完成");
    assert_eq!(attempts.load(Ordering::SeqCst), 3, "共 3 次补偿尝试");
}

#[test]
fn duplicate_start_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tx6").unwrap();
    assert!(c.start("tx6").is_err(), "重复登记被拒（幂等键 tx_id）");
}

// -----------------------------------------------------------------------
// 13.5 SAGA 补偿协议：中间态崩溃恢复（13.5.3）+ 超时屏障空转（13.5.2）
// 中间态用「构造磁盘状态文件」模拟崩溃点（SagaState 可序列化，等价真实崩溃）
// -----------------------------------------------------------------------

/// 直接写磁盘状态文件（模拟网关在该状态崩溃后的恢复起点）。
fn write_state(dir: &std::path::Path, st: &SagaState) {
    std::fs::write(
        dir.join(format!("saga-{}.json", st.tx_id)),
        serde_json::to_string(st).unwrap(),
    )
    .unwrap();
}

#[test]
fn executing_midway_resume_forward() {
    // 13.5.3「正向执行中（部分登记）」：a 已登记、b 未执行时崩溃 → 重开 run → 续跑 b，不重复 a
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("tx7");
    st.status = SagaStatus::Executing; // 崩溃点：a 已登记、正向未完成
    st.executed_steps = vec!["a".to_string()];
    write_state(dir.path(), &st);

    // 重开（崩溃恢复）→ 提供完整步骤 a+b
    FWD_CALLS.store(0, Ordering::SeqCst);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let full: Vec<Box<dyn SagaStep>> = vec![
        Box::new(SimpleStep { name: "a", fail_forward: false }),
        Box::new(SimpleStep { name: "b", fail_forward: false }),
    ];
    let s = c.run("tx7", &refs(&full)).unwrap();
    assert_eq!(s, SagaStatus::Succeeded, "续跑正向完成");
    assert_eq!(FWD_CALLS.load(Ordering::SeqCst), 1, "已登记 a 不重复执行，只执行 b");
    assert_eq!(
        c.status("tx7").unwrap().executed_steps,
        vec!["a".to_string(), "b".to_string()]
    );
}

#[test]
fn failed_state_resume_compensates() {
    // 13.5.3「正向失败后、补偿完成前（Failed）」：磁盘状态 Failed → 重开 run → 补偿完成
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("tx8");
    st.status = SagaStatus::Failed;
    st.executed_steps = vec!["a".to_string()];
    st.last_error = Some("业务失败".into());
    write_state(dir.path(), &st);

    CMP_CALLS.store(0, Ordering::SeqCst);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let steps: Vec<Box<dyn SagaStep>> =
        vec![Box::new(SimpleStep { name: "a", fail_forward: false })];
    let s = c.run("tx8", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensated, "Failed 恢复 → 续补偿完成");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 1, "补偿已登记分支 a");
    assert!(c.status("tx8").unwrap().last_error.is_none(), "终态清空 last_error");
}

#[test]
fn compensating_partial_resume() {
    // 13.5.3「Compensating 中（部分已补偿）」：a 已补偿、b 未补偿 → 重开 run → 续补偿 b
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("tx9");
    st.status = SagaStatus::Compensating;
    st.executed_steps = vec!["a".to_string(), "b".to_string()];
    st.compensated_steps.insert("a".to_string());
    write_state(dir.path(), &st);

    CMP_CALLS.store(0, Ordering::SeqCst);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let steps: Vec<Box<dyn SagaStep>> = vec![
        Box::new(SimpleStep { name: "a", fail_forward: false }),
        Box::new(SimpleStep { name: "b", fail_forward: false }),
    ];
    let s = c.run("tx9", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensated, "续补偿剩余分支完成");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 1, "a 已补偿不重复，只补 b");
}

#[test]
fn missing_step_definition_keeps_compensating() {
    // 13.5 修复：已登记分支缺补偿定义 → 保持 Compensating（不得静默 Compensated）
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("tx10");
    st.status = SagaStatus::Compensating;
    st.executed_steps = vec!["a".to_string()];
    write_state(dir.path(), &st);

    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    // 本次 steps 缺 a（只有 b）→ 补偿无法执行 a → 保持 Compensating + last_error
    let steps: Vec<Box<dyn SagaStep>> =
        vec![Box::new(SimpleStep { name: "b", fail_forward: false })];
    let s = c.run("tx10", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensating, "缺步骤定义不得终态");
    let st = c.status("tx10").unwrap();
    assert!(st.last_error.as_deref().unwrap().contains("缺少补偿定义"), "{:?}", st.last_error);
}

// -----------------------------------------------------------------------
// 补充：缺步骤定义修复（170bf21）变体覆盖
// -----------------------------------------------------------------------

#[test]
fn missing_definition_direct_compensate_call() {
    // 直接调 compensate()（不经 run）同样保持 Compensating（修复作用点本身）
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("tx12");
    st.status = SagaStatus::Executing;
    st.executed_steps = vec!["a".to_string()];
    write_state(dir.path(), &st);

    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    // steps 缺 a → compensate 无法执行补偿
    let steps: Vec<Box<dyn SagaStep>> =
        vec![Box::new(SimpleStep { name: "b", fail_forward: false })];
    let s = c.compensate("tx12", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensating, "直接补偿路径同样保持 Compensating");
    let st = c.status("tx12").unwrap();
    assert!(st.last_error.as_deref().unwrap().contains("缺少补偿定义"), "{:?}", st.last_error);
}

#[test]
fn missing_definition_partial_progress_kept() {
    // 逆序补偿：b 有定义先补偿成功登记；a 缺定义 → 保持 Compensating，部分进度不丢
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("tx13");
    st.status = SagaStatus::Compensating;
    st.executed_steps = vec!["a".to_string(), "b".to_string()];
    write_state(dir.path(), &st);

    CMP_CALLS.store(0, Ordering::SeqCst);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    // 只提供 b 的定义（缺 a）→ 逆序先补 b（成功），a 缺定义 → Compensating
    let steps: Vec<Box<dyn SagaStep>> =
        vec![Box::new(SimpleStep { name: "b", fail_forward: false })];
    let s = c.compensate("tx13", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensating, "部分缺定义 → 保持 Compensating");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 1, "b 已被补偿");
    let st = c.status("tx13").unwrap();
    assert!(st.compensated_steps.contains("b"), "b 补偿进度已登记");
    assert!(!st.compensated_steps.contains("a"), "a 未补偿");
    assert!(st.last_error.as_deref().unwrap().contains("缺少补偿定义"), "{:?}", st.last_error);
}

#[test]
fn missing_definition_retry_then_compensated() {
    // 缺定义 → 补全定义重试 → 续补偿剩余分支 → Compensated（不重复已补偿）
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("tx14");
    st.status = SagaStatus::Compensating;
    st.executed_steps = vec!["a".to_string(), "b".to_string()];
    write_state(dir.path(), &st);

    CMP_CALLS.store(0, Ordering::SeqCst);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let partial: Vec<Box<dyn SagaStep>> =
        vec![Box::new(SimpleStep { name: "b", fail_forward: false })];
    let s = c.compensate("tx14", &refs(&partial)).unwrap();
    assert_eq!(s, SagaStatus::Compensating, "首次缺 a 定义");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 1);

    // 补全定义重试 → 续补 a（b 已补偿不重复）
    let full: Vec<Box<dyn SagaStep>> = vec![
        Box::new(SimpleStep { name: "a", fail_forward: false }),
        Box::new(SimpleStep { name: "b", fail_forward: false }),
    ];
    let s = c.compensate("tx14", &refs(&full)).unwrap();
    assert_eq!(s, SagaStatus::Compensated, "补全定义后补偿完成");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 2, "第二次只补 a");
    let st = c.status("tx14").unwrap();
    assert!(st.compensated_steps.contains("a") && st.compensated_steps.contains("b"));
    assert!(st.last_error.is_none(), "终态清空错误");
}

#[test]
fn missing_definition_state_persists_across_reopen() {
    // 缺定义 → Compensating + last_error 持久化到磁盘，重开协调器可见（对账/回查依据）
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    {
        let mut st = SagaState::new("tx15");
        st.status = SagaStatus::Compensating;
        st.executed_steps = vec!["a".to_string()];
        write_state(dir.path(), &st);
        let mut c = SagaCoordinator::open(dir.path()).unwrap();
        let steps: Vec<Box<dyn SagaStep>> =
            vec![Box::new(SimpleStep { name: "b", fail_forward: false })];
        c.compensate("tx15", &refs(&steps)).unwrap();
    } // 协调器丢弃 = 网关崩溃/重启
    let c2 = SagaCoordinator::open(dir.path()).unwrap();
    let st = c2.status("tx15").unwrap();
    assert_eq!(st.status, SagaStatus::Compensating, "重启后仍 Compensating（未误终态）");
    assert!(st.last_error.as_deref().unwrap().contains("缺少补偿定义"), "{:?}", st.last_error);
}

#[test]
fn compensate_on_terminal_is_noop() {
    // 终态（Succeeded/Compensated）调 compensate → 直接返回终态，状态不变
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("tx16");
    st.status = SagaStatus::Succeeded;
    st.executed_steps = vec!["a".to_string()];
    write_state(dir.path(), &st);

    CMP_CALLS.store(0, Ordering::SeqCst);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let steps: Vec<Box<dyn SagaStep>> =
        vec![Box::new(SimpleStep { name: "a", fail_forward: false })];
    let s = c.compensate("tx16", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Succeeded, "终态 compensate no-op");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 0, "终态不发起补偿");
    let st = c.status("tx16").unwrap();
    assert!(st.compensated_steps.is_empty(), "终态不登记补偿进度");
    assert_eq!(st.last_error, None);
}

/// 慢业务节点：接受连接后 sleep 再响应（客户端超时前不应收到响应）。
fn slow_node() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            std::thread::sleep(Duration::from_millis(300));
            let _ = s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        }
    });
    format!("http://{addr}")
}

#[test]
fn timeout_unregistered_step_not_compensated() {
    // 13.5.2 超时不确定性：慢节点超时（50ms < 300ms 响应）→ 该步未登记 →
    // 屏障空转不补偿（宁可漏补偿，不可错补偿）；已登记分支正常逆序补偿
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tx11").unwrap();
    let base = slow_node();
    let steps: Vec<Box<dyn SagaStep>> = vec![
        Box::new(SimpleStep { name: "fast", fail_forward: false }),
        Box::new(
            HttpStep::new("slow", format!("{base}/slow/action"), format!("{base}/slow/compensate"), vec![])
                .with_timeout(50),
        ),
    ];
    FWD_CALLS.store(0, Ordering::SeqCst);
    CMP_CALLS.store(0, Ordering::SeqCst);
    let s = c.run("tx11", &refs(&steps)).unwrap();
    assert_eq!(s, SagaStatus::Compensated, "超时失败 → 逆序补偿完成");
    let st = c.status("tx11").unwrap();
    assert_eq!(st.executed_steps, vec!["fast"], "超时未登记分支不在 executed_steps（屏障空转依据）");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 1, "只补偿已登记 fast");
    assert!(st.last_error.is_none(), "补偿完成后终态清空错误（终态语义）");
}

// -----------------------------------------------------------------------
// 13.6 拓扑并行（topo_layers + run_parallel）+ 13.7 对账器（retry_pending）
// -----------------------------------------------------------------------

#[test]
fn topo_layers_ordering_cycle_and_invalid() {
    // 依赖：1/2 依赖 0 → 层 [[0],[1,2]]（层内可并行）
    let layers = topo_layers(3, &[vec![], vec![0], vec![0]]).unwrap();
    assert_eq!(layers, vec![vec![0], vec![1, 2]]);
    // 链式 a→b→c → 逐层
    let layers = topo_layers(3, &[vec![], vec![0], vec![1]]).unwrap();
    assert_eq!(layers, vec![vec![0], vec![1], vec![2]]);
    // 环 0↔1
    assert!(topo_layers(2, &[vec![1], vec![0]]).is_err(), "环应报错");
    // 自依赖
    assert!(topo_layers(2, &[vec![0], vec![]]).is_err(), "自依赖应报错");
    // 非法索引
    assert!(topo_layers(2, &[vec![5], vec![]]).is_err(), "越界依赖应报错");
    // 长度不匹配
    assert!(topo_layers(3, &[vec![], vec![]]).is_err(), "依赖数 ≠ 步骤数应报错");
}

#[test]
fn run_parallel_respects_dependency_order() {
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tp1").unwrap();
    let order = std::sync::Arc::new(Mutex::new(Vec::new()));
    let o1 = order.clone();
    let o2 = order.clone();
    let steps: Vec<Box<dyn SagaStep>> = vec![
        Box::new(ClosureStep::new(
            "a",
            move || {
                o1.lock().unwrap().push("a");
                Ok(())
            },
            || Ok(()),
        )),
        Box::new(ClosureStep::new(
            "b",
            move || {
                o2.lock().unwrap().push("b");
                Ok(())
            },
            || Ok(()),
        )),
    ];
    let deps: Vec<Vec<usize>> = vec![vec![], vec![0]]; // b 依赖 a
    let s = c.run_parallel("tp1", &refs(&steps), &deps).unwrap();
    assert_eq!(s, SagaStatus::Succeeded);
    assert_eq!(*order.lock().unwrap(), vec!["a", "b"], "依赖序：a 先于 b");
    assert_eq!(c.status("tp1").unwrap().executed_steps, vec!["a", "b"]);
}

#[test]
fn run_parallel_executes_independent_steps_concurrently() {
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tp2").unwrap();
    let steps: Vec<Box<dyn SagaStep>> = vec![
        Box::new(ClosureStep::new("x", || {
            std::thread::sleep(Duration::from_millis(80));
            Ok(())
        }, || Ok(()))),
        Box::new(ClosureStep::new("y", || {
            std::thread::sleep(Duration::from_millis(80));
            Ok(())
        }, || Ok(()))),
    ];
    let deps: Vec<Vec<usize>> = vec![vec![], vec![]]; // 无依赖 → 同层并行
    let t0 = std::time::Instant::now();
    let s = c.run_parallel("tp2", &refs(&steps), &deps).unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(s, SagaStatus::Succeeded);
    assert!(elapsed.as_millis() < 150, "并行应远小于串行 160ms: {elapsed:?}");
}

#[test]
fn run_parallel_failure_compensates_in_reverse_topo() {
    // 链 a→b→c；b 失败 → 仅已登记 a 被补偿，c 未执行不补偿
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    c.start("tp3").unwrap();
    let steps: Vec<Box<dyn SagaStep>> = vec![
        Box::new(SimpleStep { name: "a", fail_forward: false }),
        Box::new(SimpleStep { name: "b", fail_forward: true }),
        Box::new(SimpleStep { name: "c", fail_forward: false }),
    ];
    let deps: Vec<Vec<usize>> = vec![vec![], vec![0], vec![1]];
    FWD_CALLS.store(0, Ordering::SeqCst);
    CMP_CALLS.store(0, Ordering::SeqCst);
    let s = c.run_parallel("tp3", &refs(&steps), &deps).unwrap();
    assert_eq!(s, SagaStatus::Compensated, "链中段失败 → 补偿完成");
    assert_eq!(c.status("tp3").unwrap().executed_steps, vec!["a"], "仅 a 已登记");
    assert_eq!(CMP_CALLS.load(Ordering::SeqCst), 1, "只补偿已登记 a");
}

#[test]
fn retry_pending_compensates_failed_with_counter() {
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("rp1");
    st.status = SagaStatus::Failed;
    st.executed_steps = vec!["a".to_string()];
    write_state(dir.path(), &st);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let n = c.retry_pending(
        |_tx| vec![Box::new(SimpleStep { name: "a", fail_forward: false })],
        1_000,
        60_000,
        300_000,
    );
    assert_eq!(n, 1, "Failed 事务被自动续补偿");
    let st = c.status("rp1").unwrap().clone();
    assert_eq!(st.status, SagaStatus::Compensated);
    assert_eq!(st.retry_count, 1, "重试计数 +1");
    assert!(st.last_retry_at_ms.is_some(), "记录最后重试时间");
}

#[test]
fn retry_pending_backoff_skips_recent_retry() {
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("rp2");
    st.status = SagaStatus::Compensating;
    st.executed_steps = vec!["a".to_string()];
    st.retry_count = 1; // 退避 = 2s
    st.last_retry_at_ms = Some(900); // 距 now=1000 仅 100ms < 2000ms → 跳过
    write_state(dir.path(), &st);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let n = c.retry_pending(
        |_tx| vec![Box::new(SimpleStep { name: "a", fail_forward: false })],
        1_000,
        60_000,
        300_000,
    );
    assert_eq!(n, 0, "退避期内跳过");
    assert_eq!(c.status("rp2").unwrap().status, SagaStatus::Compensating, "状态不变");
}

#[test]
fn retry_pending_stalls_executing_then_compensates() {
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("rp3");
    st.status = SagaStatus::Executing;
    st.executed_steps = vec!["a".to_string()];
    st.updated_at_ms = 100; // 距 now 远超 60s 阈值 → 判定挂起
    write_state(dir.path(), &st);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let n = c.retry_pending(
        |_tx| vec![Box::new(SimpleStep { name: "a", fail_forward: false })],
        10_000_000,
        60_000,
        300_000,
    );
    assert_eq!(n, 1, "挂起的 Executing 被标记失败并补偿");
    let st = c.status("rp3").unwrap().clone();
    assert_eq!(st.status, SagaStatus::Compensated);
    assert!(st.last_error.is_none(), "补偿完成终态清空（含挂起标记）");
}

#[test]
fn retry_pending_skips_without_step_definitions() {
    let _g = TEST_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut st = SagaState::new("rp4");
    st.status = SagaStatus::Failed;
    st.executed_steps = vec!["a".to_string()];
    write_state(dir.path(), &st);
    let mut c = SagaCoordinator::open(dir.path()).unwrap();
    let n = c.retry_pending(|_tx| Vec::new(), 1_000, 60_000, 300_000);
    assert_eq!(n, 0, "无步骤定义 → 跳过（留人工/补发定义）");
    assert_eq!(c.status("rp4").unwrap().status, SagaStatus::Failed, "不推进状态");
}
