//! SAGA 编排 + 补偿状态机（Ex-2，design_extension v0.1 L2 跨分片业务事务）。
//!
//! 长事务拆 N 个 docid 级本地事务（步骤），任一步失败反向补偿；状态机持久化
//! （JSON tmp+rename 原子写，复用 MvScheduler 模式）；屏障（Barrier）防空回滚/
//! 悬挂：分支登记（正向成功后记录）先于补偿、补偿幂等键（tx_id+step）、回查接口
//! transactionId→status 持久化（网关 `/saga/status` 依据）。
//!
//! 设计要点（design_extension L2）：
//! - **正向**：按序执行步骤；成功即登记分支（executed_steps）；
//! - **反向补偿**：任一步失败 → 对已登记分支**逆序**补偿（补偿 = 语义相反的新操作，幂等）；
//! - **空回滚防护**：补偿只作用于已登记分支——超时未执行的分支不补偿（宁可多发由屏障空转）；
//! - **悬挂防护**：终态/已补偿分支拒绝迟到正向执行（防重复应用）；
//! - **崩溃恢复**：状态持久化，协调器重建后从持久化进度续跑/续补偿。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// SAGA 状态机状态（终态 = Succeeded / Compensated）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SagaStatus {
    /// 已创建（登记，屏障回查起点），待执行。
    Init,
    /// 正向执行中。
    Executing,
    /// 全部正向成功（终态）。
    Succeeded,
    /// 正向失败，待补偿。
    Failed,
    /// 反向补偿中（任一补偿失败保持此态，重试续跑）。
    Compensating,
    /// 补偿完成（终态）。
    Compensated,
}

impl SagaStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Compensated)
    }
}

/// 持久化状态（transactionId → status 回查 + 崩溃恢复续跑）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaState {
    pub tx_id: String,
    pub status: SagaStatus,
    /// 正向已执行（已登记）步骤，顺序 = 执行序，反向补偿依据。
    pub executed_steps: Vec<String>,
    /// 已补偿步骤（补偿幂等：重复补偿 no-op）。
    pub compensated_steps: BTreeSet<String>,
    /// 最后错误（诊断/回查展示）。
    pub last_error: Option<String>,
}

impl SagaState {
    pub fn new(tx_id: &str) -> Self {
        Self {
            tx_id: tx_id.to_string(),
            status: SagaStatus::Init,
            executed_steps: Vec::new(),
            compensated_steps: BTreeSet::new(),
            last_error: None,
        }
    }
}

/// SAGA 步骤：正向 + 反向补偿（业务方实现；补偿 = 语义相反的新操作，须幂等）。
pub trait SagaStep {
    /// 步骤标识（屏障幂等键 = tx_id + name）。
    fn name(&self) -> &str;
    /// 正向执行（docid 级本地事务）。
    fn forward(&self) -> Result<()>;
    /// 反向补偿（幂等：重复调用不叠加副作用）。
    fn compensate(&self) -> Result<()>;
}

/// 闭包式步骤（测试/简单场景：直接给正向与补偿闭包）。
pub struct ClosureStep {
    name: String,
    forward: Box<dyn Fn() -> Result<()> + Send + Sync>,
    compensate: Box<dyn Fn() -> Result<()> + Send + Sync>,
}

impl ClosureStep {
    pub fn new(
        name: impl Into<String>,
        forward: impl Fn() -> Result<()> + Send + Sync + 'static,
        compensate: impl Fn() -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            forward: Box::new(forward),
            compensate: Box::new(compensate),
        }
    }
}

impl SagaStep for ClosureStep {
    fn name(&self) -> &str {
        &self.name
    }
    fn forward(&self) -> Result<()> {
        (self.forward)()
    }
    fn compensate(&self) -> Result<()> {
        (self.compensate)()
    }
}

/// SAGA 协调器：按序执行步骤 + 失败反向补偿 + 屏障 + 状态持久化续跑。
///
/// 持久化：`{dir}/saga-{tx_id}.json`（tmp + rename 原子写，重启加载全部续跑）。
pub struct SagaCoordinator {
    dir: PathBuf,
    states: BTreeMap<String, SagaState>,
}

impl SagaCoordinator {
    /// 打开协调器目录（恢复全部 saga-*.json 状态）。
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut states = BTreeMap::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            for entry in rd.flatten() {
                let fname = entry.file_name().to_string_lossy().into_owned();
                if let Some(stem) = fname
                    .strip_prefix("saga-")
                    .and_then(|s| s.strip_suffix(".json"))
                {
                    let text = std::fs::read_to_string(entry.path())?;
                    match serde_json::from_str::<SagaState>(&text) {
                        Ok(st) => {
                            states.insert(stem.to_string(), st);
                        }
                        Err(e) => {
                            return Err(Error::Corrupted(format!(
                                "saga 状态损坏 {}: {e}",
                                entry.path().display()
                            )));
                        }
                    }
                }
            }
        }
        Ok(Self { dir: dir.to_path_buf(), states })
    }

    /// 登记事务（transactionId → Init），屏障回查接口持久化起点。
    pub fn start(&mut self, tx_id: &str) -> Result<SagaState> {
        if self.states.contains_key(tx_id) {
            return Err(Error::Config(format!("SAGA 事务已存在: {tx_id}")));
        }
        let st = SagaState::new(tx_id);
        self.persist(&st)?; // persist 已同步内存态
        Ok(st)
    }

    /// 回查：transactionId → status（/saga/status 依据）。
    pub fn status(&self, tx_id: &str) -> Option<&SagaState> {
        self.states.get(tx_id)
    }

    /// 全部事务状态（对账/管理）。
    pub fn all_states(&self) -> impl Iterator<Item = &SagaState> {
        self.states.values()
    }

    /// 启动/续跑：从当前状态继续正向执行；任一步失败 → 反向补偿；
    /// 已 Failed/Compensating → 续补偿（重试）。返回终态状态。
    pub fn run(&mut self, tx_id: &str, steps: &[&dyn SagaStep]) -> Result<SagaStatus> {
        let status = self.states.get(tx_id).map(|s| s.status).unwrap_or(SagaStatus::Init);
        match status {
            SagaStatus::Succeeded | SagaStatus::Compensated => return Ok(status), // 终态：迟到正向被拒
            SagaStatus::Failed | SagaStatus::Compensating => {
                return self.compensate(tx_id, steps);
            }
            _ => {}
        }
        let mut st = self.states.get(tx_id).cloned().unwrap_or_else(|| SagaState::new(tx_id));
        st.status = SagaStatus::Executing;
        for step in steps {
            // 屏障（悬挂防护）：该步已补偿过 → 拒绝迟到正向执行（防悬挂重复应用）
            if st.compensated_steps.contains(step.name()) {
                continue;
            }
            if st.executed_steps.iter().any(|n| n == step.name()) {
                continue; // 已登记（恢复续跑场景）
            }
            match step.forward() {
                Ok(()) => {
                    // 分支登记：正向成功后记录（屏障空回滚依据）
                    st.executed_steps.push(step.name().to_string());
                    self.persist(&st)?;
                }
                Err(e) => {
                    st.status = SagaStatus::Failed;
                    st.last_error = Some(e.to_string());
                    self.persist(&st)?;
                    return self.compensate(tx_id, steps);
                }
            }
        }
        st.status = SagaStatus::Succeeded;
        self.persist(&st)?;
        Ok(st.status)
    }

    /// 反向补偿：对已登记分支逆序补偿；任一补偿失败 → 保持 Compensating 待重试。
    pub fn compensate(&mut self, tx_id: &str, steps: &[&dyn SagaStep]) -> Result<SagaStatus> {
        let mut st = self.states.get(tx_id).cloned().unwrap();
        if st.status.is_terminal() {
            return Ok(st.status);
        }
        st.status = SagaStatus::Compensating;
        self.persist(&st)?;
        let by_name: BTreeMap<&str, &dyn SagaStep> =
            steps.iter().map(|s| (s.name(), *s)).collect();
        // 逆序补偿已登记分支（空回滚防护：超时未执行的分支不在 executed_steps → 不补偿）
        for name in st.executed_steps.iter().rev() {
            if st.compensated_steps.contains(name) {
                continue; // 补偿幂等
            }
            let Some(step) = by_name.get(name.as_str()) else {
                continue;
            };
            match step.compensate() {
                Ok(()) => {
                    st.compensated_steps.insert(name.clone());
                    self.persist(&st)?;
                }
                Err(e) => {
                    st.last_error = Some(e.to_string());
                    self.persist(&st)?;
                    return Ok(st.status); // Compensating：下次 run 续补偿
                }
            }
        }
        st.status = SagaStatus::Compensated;
        st.last_error = None;
        self.persist(&st)?;
        Ok(st.status)
    }

    /// 持久化（tmp + rename 原子写）并同步内存态（后续读同一状态来源）。
    fn persist(&mut self, st: &SagaState) -> Result<()> {
        let path = self.path(&st.tx_id);
        let text = serde_json::to_string(st)
            .map_err(|e| Error::Serialize(format!("SAGA 状态序列化失败: {e}")))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, text)?;
        std::fs::rename(&tmp, &path)?;
        self.states.insert(st.tx_id.clone(), st.clone());
        Ok(())
    }

    fn path(&self, tx_id: &str) -> PathBuf {
        self.dir.join(format!("saga-{tx_id}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

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
}
