//! SagaCoordinator：状态机执行 / 反向补偿 / 13.7 对账 / 持久化（原 saga.rs 主体）。
//!
//! 持久化：`{dir}/saga-{tx_id}.json`（tmp + rename 原子写，重启加载全部续跑）。
//! run = 按序正向执行 + 失败反向补偿；run_parallel = 13.6 拓扑分层并行；
//! retry_pending = 13.7 对账器（指数退避自动续补偿 / Executing 挂起检测）。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

use super::topology::topo_layers;
use super::{now_ms, SagaState, SagaStatus, SagaStep};

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
                // 13.5：已登记分支必须可补偿——步骤定义缺失时保持 Compensating 待重试，
                // 不得静默标记 Compensated（未补偿分支不能终态）。
                st.last_error = Some(format!(
                    "步骤 {name} 缺少补偿定义（本次 steps 未提供 compensate_url），保持待补偿"
                ));
                self.persist(&st)?;
                return Ok(st.status); // Compensating
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

    /// 13.6 拓扑并行执行：按步骤依赖 DAG 分层，层内并行正向、层间屏障；
    /// 失败 → 取消本层剩余 → 逆序补偿（executed_steps 按拓扑层序登记，
    /// 其反序 = 反拓扑序：依赖者先补偿、被依赖者后补偿——13.6.3）。
    /// `deps[i]` = steps[i] 依赖的步骤索引（DAG 边）；环 → Error::Config。
    /// 语义与 `run` 对齐（终态幂等 / 续跑 / Failed/Compensating 转补偿）。
    pub fn run_parallel(
        &mut self,
        tx_id: &str,
        steps: &[&dyn SagaStep],
        deps: &[Vec<usize>],
    ) -> Result<SagaStatus> {
        let status = self.states.get(tx_id).map(|s| s.status).unwrap_or(SagaStatus::Init);
        match status {
            SagaStatus::Succeeded | SagaStatus::Compensated => return Ok(status),
            SagaStatus::Failed | SagaStatus::Compensating => {
                return self.compensate(tx_id, steps);
            }
            _ => {}
        }
        // 拓扑分层（Kahn）：层内无依赖可并行；环/非法依赖 → 400
        let layers = topo_layers(steps.len(), deps)?;
        let mut st = self.states.get(tx_id).cloned().unwrap_or_else(|| SagaState::new(tx_id));
        st.status = SagaStatus::Executing;
        for layer in layers {
            // 过滤：已登记（续跑）/ 已补偿（悬挂防护）跳过
            let pending: Vec<usize> = layer
                .into_iter()
                .filter(|&idx| {
                    let n = steps[idx].name();
                    !st.compensated_steps.contains(n) && !st.executed_steps.iter().any(|x| x == n)
                })
                .collect();
            if pending.is_empty() {
                continue;
            }
            // 层内并行正向（scoped 线程）；注册顺序 = pending 顺序（层序，补偿逆序安全）
            let outcomes = std::thread::scope(|s| {
                let mut handles = Vec::new();
                for &idx in &pending {
                    let step = steps[idx];
                    handles.push(s.spawn(move || (idx, step.forward())));
                }
                handles.into_iter().map(|h| h.join().unwrap()).collect::<Vec<_>>()
            });
            for (idx, res) in &outcomes {
                let step = steps[*idx];
                match res {
                    Ok(()) => {
                        if !st.executed_steps.iter().any(|n| n == step.name()) {
                            st.executed_steps.push(step.name().to_string()); // 分支登记（I4）
                            self.persist(&st)?;
                        }
                    }
                    Err(e) => {
                        st.status = SagaStatus::Failed;
                        st.last_error = Some(e.to_string());
                        self.persist(&st)?;
                        return self.compensate(tx_id, steps); // 剩余本层未执行分支不登记 → 屏障空转
                    }
                }
            }
        }
        st.status = SagaStatus::Succeeded;
        self.persist(&st)?;
        Ok(st.status)
    }

    /// 13.7 对账器核心：扫描未终态事务，对可重试错误按指数退避自动续补偿。
    /// - `steps_for(tx_id)`：提供该事务的步骤定义（网关缓存的原始定义；无定义 → 跳过并告警留人工）；
    /// - `executing_stall_ms`：Executing 挂起阈值——超过视为执行无进展，标记 Failed 并触发补偿；
    /// - `max_backoff_ms`：指数退避上限（1s → 2s → 4s … 上限）。
    /// 返回本次实际触发补偿重试的事务数。
    pub fn retry_pending(
        &mut self,
        mut steps_for: impl FnMut(&str) -> Vec<Box<dyn SagaStep>>,
        now_ms: u64,
        executing_stall_ms: u64,
        max_backoff_ms: u64,
    ) -> usize {
        let txs: Vec<String> = self
            .states
            .iter()
            .filter(|(_, st)| !st.status.is_terminal())
            .map(|(id, _)| id.clone())
            .collect();
        let mut retried = 0;
        for tx in txs {
            let stall = match self.states.get(&tx) {
                Some(st) if st.status == SagaStatus::Executing => {
                    now_ms.saturating_sub(st.updated_at_ms) >= executing_stall_ms
                }
                _ => false,
            };
            if stall {
                // Executing 挂起 → 标记 Failed（last_error 注明）→ 走补偿
                let mut st = self.states.get(&tx).cloned().unwrap();
                st.status = SagaStatus::Failed;
                st.last_error = Some("对账器：正向执行挂起超阈值".into());
                if let Err(e) = self.persist(&st) {
                    tracing::warn!("对账器持久化失败 {tx}: {e}");
                    continue;
                }
            }
            let st = match self.states.get(&tx) {
                Some(st) if st.status == SagaStatus::Failed || st.status == SagaStatus::Compensating => st.clone(),
                _ => continue,
            };
            // 指数退避：wait = min(max, 1000 * 2^retry_count)
            let wait = (1000u64 << st.retry_count.min(31)).min(max_backoff_ms);
            if let Some(last) = st.last_retry_at_ms {
                if now_ms.saturating_sub(last) < wait {
                    continue; // 退避中
                }
            }
            let mut steps = steps_for(&tx);
            if steps.is_empty() {
                continue; // 无步骤定义：跳过（13.7：仅告警，人工/补发定义）
            }
            let refs: Vec<&dyn SagaStep> = steps.iter().map(|s| s.as_ref()).collect();
            match self.compensate(&tx, &refs) {
                Ok(_) => {
                    if let Some(mut st) = self.states.get(&tx).cloned() {
                        st.retry_count = st.retry_count.saturating_add(1);
                        st.last_retry_at_ms = Some(now_ms);
                        let _ = self.persist(&st);
                    }
                    retried += 1;
                }
                Err(e) => {
                    tracing::warn!("对账器补偿失败 {tx}: {e}");
                }
            }
        }
        retried
    }

    /// 持久化（tmp + rename 原子写）并同步内存态（后续读同一状态来源）。
    fn persist(&mut self, st: &SagaState) -> Result<()> {
        let mut st = st.clone();
        st.updated_at_ms = now_ms(); // 13.7 挂起检测：状态最后变更时间
        let path = self.path(&st.tx_id);
        let text = serde_json::to_string(&st)
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
