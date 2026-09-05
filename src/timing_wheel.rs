//! Task-007 层级时间轮基础框架（development_remain Task-007，2026-09-05）。
//!
//! 秒/分/时/天 四级：桶数 `60 + 60 + 24 + 365 = 509`。接口：
//! - [`schedule`](TimingWheel::schedule)(delay, kind, action) → [`TaskHandle`]（可取消）；
//! - [`tick`](TimingWheel::tick)：推进指针（1 秒步进），执行到期任务；
//! - [`advance`](TimingWheel::advance)(secs)：批量推进（按"下一事件边界"跳步，空段 O(1)）；
//! - 取消：handle 可从任意轮桶摘除（不执行、不持久化）；
//! - 检查点：每 `CHECKPOINT_EVERY_SECS`（600s = 10 分钟）落盘当前指针 + 未完成任务的
//!   绝对到期时刻与 kind 元数据（`checkpoint.json`，tmp+rename 原子）；
//! - 冷启动：`load_checkpoint` 恢复指针与任务元数据 → 到期时间已过就地执行（由调用方
//!   重注册 action），未过重排入轮。
//!
//! 挂载权衡（development_remain Task-007 备注，2026-09-05 定）：TTL 删除沿用**按天分桶 +
//! 整目录 O(1) 删除**、Compaction/WAL 删沿用既有后台轮询调度——本框架为**基础基建**先行
//! 落地（含 509 桶结构、级联跃迁、检查点/恢复、MockClock 加速测试），后续挂载对象评估
//! 替换/改造时直接复用，不重复造轮。
//!
//! 刻度与放置：
//! - 桶编号按**绝对秒**取模（`slot = (expiry / period[w]) % size[w]`）；
//! - 到期任务进秒轮（仅本分钟内到期）；分/时/天轮在各自"整点"级联下放一级：
//!   天轮桶 → 时轮桶（当天内到期）→ 分轮桶（本小时内）→ 秒轮桶（本分钟内）→ 到期执行。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// 秒/分/时/天 四级桶容量。
pub const WHEEL_SIZES: [usize; 4] = [60, 60, 24, 365];
/// 每级桶的刻度周期（秒）。
pub const WHEEL_PERIOD_S: [u64; 4] = [1, 60, 3600, 86_400];
/// 总桶数（验收口径：60+60+24+365 = 509）。
pub const TOTAL_BUCKETS: usize = 60 + 60 + 24 + 365;

/// 检查点周期（10 分钟）。
pub const CHECKPOINT_EVERY_SECS: u64 = 600;

/// 任务 id（TaskHandle）。
type TaskId = u64;

/// 到期任务元数据（检查点持久化 / 冷启动重建）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScheduledTask {
    pub id: u64,
    /// 绝对到期秒（epoch）。
    pub expiry_sec: u64,
    /// 语义标签（调用方按 kind 重建 action）。
    pub kind: String,
}

/// 轮内任务：到期元数据 + 可选执行闭包（内存态；重启后由调用方按 kind 重注册）。
struct WheelTask {
    meta: ScheduledTask,
    action: Option<Box<dyn FnOnce() + Send>>,
}

/// 层级时间轮（秒/分/时/天四级，509 桶）。
pub struct TimingWheel {
    /// 当前指针（绝对秒）。
    now: u64,
    next_id: u64,
    /// 四级轮：wheels[w][slot] = 该桶任务。
    wheels: [Vec<Vec<WheelTask>>; 4],
    /// 每级轮"有任务的桶"集合（advance 跳步/级联候选）。
    occupied: [BTreeSet<usize>; 4],
    /// 未完成任务 id → 到期秒（取消定位）。
    live: BTreeMap<u64, u64>,
    /// 下一到期秒（去重）→ 该秒任务数（advance 跳步）。
    expiry_set: BTreeMap<u64, usize>,
    /// 距上次检查点的累计推进秒。
    since_checkpoint: u64,
    checkpoint_path: Option<PathBuf>,
}

/// 调度句柄（任务 id；可用于取消）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskHandle(pub u64);

impl TaskHandle {
    pub fn id(&self) -> u64 {
        self.0
    }
}

impl TimingWheel {
    /// 新建（`now_sec` = 当前绝对秒；mock 测试可传 0 起步）。
    pub fn new(now_sec: u64) -> Self {
        TimingWheel {
            now: now_sec,
            next_id: 1,
            wheels: std::array::from_fn(|w| (0..WHEEL_SIZES[w]).map(|_| Vec::new()).collect()),
            occupied: std::array::from_fn(|_| BTreeSet::new()),
            live: BTreeMap::new(),
            expiry_set: BTreeMap::new(),
            since_checkpoint: 0,
            checkpoint_path: None,
        }
    }

    /// 挂载检查点文件路径（`enable_checkpoint(path)`）。
    pub fn enable_checkpoint(&mut self, path: impl Into<PathBuf>) {
        self.checkpoint_path = Some(path.into());
    }

    /// 当前指针（绝对秒）。
    pub fn now(&self) -> u64 {
        self.now
    }

    /// 当前累计未完成任务数。
    pub fn pending(&self) -> usize {
        self.live.len()
    }

    /// 各轮在途任务数（诊断/测试）。
    pub fn per_wheel_pending(&self) -> [usize; 4] {
        [
            self.wheels[0].iter().map(|b| b.len()).sum(),
            self.wheels[1].iter().map(|b| b.len()).sum(),
            self.wheels[2].iter().map(|b| b.len()).sum(),
            self.wheels[3].iter().map(|b| b.len()).sum(),
        ]
    }

    /// 注册延迟任务。`delay_sec` 需 ≥ 1；`kind` 供检查点/冷启动重建；`action=None`
    /// 表示仅登记元数据（如冷启动后由调用方按 kind 重挂）。
    /// 返回 TaskHandle（可 `cancel`）。
    pub fn schedule(
        &mut self,
        delay_sec: u64,
        kind: impl Into<String>,
        action: Option<Box<dyn FnOnce() + Send>>,
    ) -> TaskHandle {
        let delay = delay_sec.max(1);
        let expiry = self.now + delay;
        let id = self.next_id;
        self.next_id += 1;
        let meta = ScheduledTask { id, expiry_sec: expiry, kind: kind.into() };
        self.insert(WheelTask { meta, action });
        *self.expiry_set.entry(expiry).or_insert(0) += 1;
        TaskHandle(id)
    }

    /// 把任务放入正确的层级桶（尽量低层级放不下就逐级上移）。
    /// 注：仅维护 live/occupied/桶归属；`expiry_set`（推进跳步）由 schedule / fire /
    /// cancel / restore 统一维护——级联下放（此处复用 insert）不改变在途任务数。
    fn insert(&mut self, task: WheelTask) {
        let id = task.meta.id;
        let expiry = task.meta.expiry_sec;
        let w = self.wheel_for(expiry);
        let slot = (expiry / WHEEL_PERIOD_S[w]) as usize % WHEEL_SIZES[w];
        self.wheels[w][slot].push(task);
        self.occupied[w].insert(slot);
        self.live.insert(id, expiry);
    }

    /// 计算到期秒应放层级：秒（本分钟内）< 分（本小时内）< 时（当天内）< 天。
    fn wheel_for(&self, expiry: u64) -> usize {
        let rem = expiry.saturating_sub(self.now);
        if rem < 60 {
            0
        } else if rem < 3600 {
            1
        } else if rem < 86_400 {
            2
        } else {
            3
        }
    }

    /// 取消任务（未执行/未到期则摘除；已执行 no-op）。返回是否命中。
    pub fn cancel(&mut self, h: TaskHandle) -> bool {
        let Some(expiry) = self.live.remove(&h.0) else {
            return false;
        };
        // 移除 expiry_set 计数
        if let Some(c) = self.expiry_set.get_mut(&expiry) {
            *c -= 1;
            if *c == 0 {
                self.expiry_set.remove(&expiry);
            }
        }
        // 全轮桶扫描摘除（桶很小；取消低频）
        for w in 0..4 {
            let period = WHEEL_PERIOD_S[w];
            let slot = (expiry / period) as usize % WHEEL_SIZES[w];
            let bucket = &mut self.wheels[w][slot];
            let before = bucket.len();
            bucket.retain(|t| t.meta.id != h.0);
            if bucket.is_empty() {
                self.occupied[w].remove(&slot);
            }
            if bucket.len() != before {
                return true;
            }
        }
        false
    }

    /// 推进指针 1 秒（逐秒刻度：前台调用方按秒驱动）。
    pub fn tick(&mut self) {
        self.advance_to(self.now + 1);
    }

    /// 批量推进 `secs` 秒（按"下一事件边界"跳步，空段零开销；测试 MockClock 加速用）。
    pub fn advance(&mut self, secs: u64) {
        let target = self.now.saturating_add(secs);
        self.advance_to(target);
    }

    fn advance_to(&mut self, target: u64) {
        loop {
            if self.now >= target {
                break;
            }
            // 下一事件（到期秒 / 级联整点）；无 → 整体快进到目标
            let Some(t) = self.next_event_at() else {
                self.since_checkpoint += target - self.now;
                self.now = target;
                break;
            };
            if t > target {
                self.since_checkpoint += target - self.now;
                self.now = target;
                break;
            }
            if t == target {
                // 事件恰好落在目标点：跳到目标并处理（到期/级联）后再结束
                self.since_checkpoint += target - self.now;
                self.now = target;
                self.process_at(self.now);
                break;
            }
            if t > self.now {
                self.since_checkpoint += t - self.now;
                self.now = t;
            }
            // 到达事件点（t<=now：到期或整点级联）→ 处理（级联 + 到期触发）
            self.process_at(self.now);
            // 防御：事件未被处理消除（异常状态）→ 强制前进 1 秒，防死循环
            if self.next_event_at().is_some_and(|n| n <= self.now) {
                self.since_checkpoint += 1;
                self.now += 1;
            }
        }
        self.maybe_checkpoint();
    }

    /// 下一事件绝对秒：min( 未完成任务最早到期秒, 各级轮非空桶的下一个级联整点 )。
    /// 无 → None（可整体快进）。
    fn next_event_at(&self) -> Option<u64> {
        let mut cand: Option<u64> = self.expiry_set.keys().next().copied();
        for w in 1..4 {
            let period = WHEEL_PERIOD_S[w];
            let size = WHEEL_SIZES[w];
            for &slot in &self.occupied[w] {
                // 该桶下一次整点级联（严格 > now）：t 满足 t%period==0 且 (t/period)%size==slot
                let per = self.now / period;
                let mut k = per;
                loop {
                    if (k as usize % size) == slot {
                        let t = k * period;
                        if t > self.now {
                            cand = Some(match cand {
                                Some(c) => c.min(t),
                                None => t,
                            });
                            break;
                        }
                    }
                    k += 1;
                    if k > per + size as u64 {
                        break; // 本周期内必有一致余数；防御越界
                    }
                }
            }
        }
        cand
    }

    /// 处理时刻 `now`：先执行整点级联（天→时→分→秒），再触发秒轮到期桶。
    fn process_at(&mut self, now: u64) {
        if now % 86_400 == 0 {
            self.cascade(3, now); // 天轮桶 → 时轮
        }
        if now % 3600 == 0 {
            self.cascade(2, now); // 时轮桶 → 分轮
        }
        if now % 60 == 0 {
            self.cascade(1, now); // 分轮桶 → 秒轮
        }
        self.fire_seconds(now);
    }

    /// 整点级联：把 w 轮当前桶（整点索引）的任务下放一级（低层级可再放秒轮）。
    fn cascade(&mut self, w: usize, now: u64) {
        debug_assert!(now % WHEEL_PERIOD_S[w] == 0, "级联须在整点");
        let period = WHEEL_PERIOD_S[w];
        let size = WHEEL_SIZES[w];
        let slot = ((now / period) % size as u64) as usize;
        let tasks: Vec<WheelTask> = std::mem::take(&mut self.wheels[w][slot]);
        if !tasks.is_empty() {
            self.occupied[w].remove(&slot);
            for t in tasks {
                self.insert(t); // 重新按剩余到期归属下级轮（秒/分/时）
            }
        }
    }

    /// 触发秒轮到期桶（该桶内任务到期秒 == now）。
    fn fire_seconds(&mut self, now: u64) {
        let slot = (now % 60) as usize;
        // 精确到期判定：秒轮桶内任务的绝对到期 == now（同分钟余数唯一）
        let tasks: Vec<WheelTask> = std::mem::take(&mut self.wheels[0][slot]);
        if !tasks.is_empty() {
            self.occupied[0].remove(&slot);
            for t in tasks {
                if t.meta.expiry_sec > now {
                    // 理论不发生（放置/级联保证 ≤ now）；防御性塞回
                    self.insert(t);
                    continue;
                }
                self.live.remove(&t.meta.id);
                if let Some(c) = self.expiry_set.get_mut(&t.meta.expiry_sec) {
                    *c -= 1;
                    if *c == 0 {
                        self.expiry_set.remove(&t.meta.expiry_sec);
                    }
                }
                if let Some(action) = t.action {
                    action();
                }
            }
        }
    }

    /// 检查点落盘（每 CHECKPOINT_EVERY_SECS 秒 / 显式调用）：指针 + 未完成任务元数据。
    pub fn checkpoint(&mut self) -> std::io::Result<()> {
        self.since_checkpoint = 0;
        let Some(path) = &self.checkpoint_path else {
            return Ok(());
        };
        let mut pending: Vec<ScheduledTask> = self
            .wheels
            .iter()
            .flat_map(|w| w.iter().flatten().map(|t| t.meta.clone()))
            .collect();
        pending.sort_by_key(|t| t.expiry_sec);
        let snap = Checkpoint { now: self.now, tasks: pending };
        let text = serde_json::to_string_pretty(&snap)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        if let Some(p) = path.parent() {
            fs::create_dir_all(p)?;
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, text)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    fn maybe_checkpoint(&mut self) {
        if self.checkpoint_path.is_some() && self.since_checkpoint >= CHECKPOINT_EVERY_SECS {
            let _ = self.checkpoint();
        }
    }

    /// 冷启动恢复：读检查点 → 指针取检查点 now（若早于真实时钟由调用方 advance 追赶）；
    /// 返回未完成任务元数据（到期已过者由调用方决定就地执行；未过按剩余重排）。
    pub fn load_checkpoint(path: &Path) -> std::io::Result<(u64, Vec<ScheduledTask>)> {
        if !path.exists() {
            return Ok((0, Vec::new()));
        }
        let text = fs::read_to_string(path)?;
        let snap: Checkpoint = serde_json::from_str(&text)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        Ok((snap.now, snap.tasks))
    }

    /// 按检查点元数据重建未完成任务（模拟重启）：`wheel` 用调用方传入时钟的 now 起步；
    /// 返回 (已过期应立刻执行的 kind 列表, 重排入轮数)。
    pub fn restore_from(
        &mut self,
        tasks: &[ScheduledTask],
        rearm: &mut dyn FnMut(u64, &str) -> Option<Box<dyn FnOnce() + Send>>,
    ) -> (usize, usize) {
        let mut overdue = 0usize;
        for t in tasks {
            let rem = t.expiry_sec.saturating_sub(self.now);
            if rem == 0 {
                overdue += 1;
                continue; // 已过期：调用方依据 kind 就地重放（此处仅登记计数）
            }
            let action = rearm(t.expiry_sec, &t.kind);
            self.insert(WheelTask {
                meta: ScheduledTask {
                    id: t.id,
                    expiry_sec: t.expiry_sec,
                    kind: t.kind.clone(),
                },
                action,
            });
            *self.expiry_set.entry(t.expiry_sec).or_insert(0) += 1;
        }
        (overdue, tasks.len() - overdue)
    }
}

impl Drop for TimingWheel {
    fn drop(&mut self) {
        // 正常关闭：落最终检查点（若启用且仍有未完成任务）
        if self.checkpoint_path.is_some() && self.pending() > 0 {
            let _ = self.checkpoint();
        }
    }
}

/// 检查点文件结构。
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Checkpoint {
    now: u64,
    tasks: Vec<ScheduledTask>,
}

// ========================= 真实时钟源（生产接入入口） =========================

/// 当前 epoch 秒（进程内统一时钟；mock 测试自行传 now）。
pub fn epoch_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ========================= 测试 =========================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};
    use std::sync::{Arc, OnceLock};

    fn tmp() -> std::path::PathBuf {
        static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let name = format!("tw-{}", SEQ.fetch_add(1, Ordering::Relaxed));
        DIR.get_or_init(|| tempfile::tempdir().unwrap())
            .path()
            .join(name)
    }

    fn fired_counter() -> (Arc<AtomicUsize>, Box<dyn FnOnce() + Send>) {
        let c = Arc::new(AtomicUsize::new(0));
        let c2 = Arc::clone(&c);
        (c, Box::new(move || {
            c2.fetch_add(1, AOrdering::Relaxed);
        }))
    }

    /// 层级放置与桶总量（60+60+24+365 = 509）
    #[test]
    fn bucket_total_and_placement() {
        assert_eq!(TOTAL_BUCKETS, 509);
        let mut w = TimingWheel::new(1_000_000);
        // 秒级（本分钟内）、分级、时级、天级各一
        let (_, a) = fired_counter();
        w.schedule(30, "sec", Some(a));
        let (_, b) = fired_counter();
        w.schedule(90, "min", Some(b));
        let (_, c) = fired_counter();
        w.schedule(3600 + 90, "hour", Some(c));
        let (_, d) = fired_counter();
        w.schedule(2 * 86_400, "day", Some(d));
        let per = w.per_wheel_pending();
        assert_eq!(per[0], 1, "30s → 秒轮");
        assert_eq!(per[1], 1, "90s → 分轮");
        assert_eq!(per[2], 1, "1h+90s → 时轮");
        assert_eq!(per[3], 1, "2 天 → 天轮");
        assert_eq!(w.pending(), 4);
    }

    /// 跨级跃迁：分轮任务到点经级联触发；时轮/天轮到点逐级下放后触发。
    #[test]
    fn cascading_across_all_levels_fires_on_time() {
        let mut w = TimingWheel::new(0);
        w.schedule(59, "a-sec-59", None); // 秒轮 59s
        w.schedule(60, "b-min-60", None); // 分轮边界 60s
        w.schedule(3_600 + 1, "c-hour-3601", None); // 时轮
        w.schedule(86_400 + 120, "d-day-86520", None); // 天轮 + 2 分钟
        // 逐秒 tick 快照层级在途数（粗验级联路径）：推进到 59s 触发秒轮任务
        w.advance(59);
        assert_eq!(w.pending(), 3, "59s 任务到期触发");
        w.advance(1); // 60s：分轮边界
        assert_eq!(w.pending(), 2, "60s 任务触发");
        // 直接推进到时轮任务到点（级联先到分轮再秒轮）
        w.advance(3_600 + 1 - 60);
        assert_eq!(w.pending(), 1, "3601s 任务触发（剩天轮任务）");
        w.advance(86_400 + 120 - (3_600 + 1));
        assert_eq!(w.pending(), 0, "天轮任务 86520s 触发");
    }

    /// 挂 action 的任务到点执行（秒/分/时/天 四级含年尺度 TTL 加速验证）。
    #[test]
    fn actions_fire_exactly_and_mock_clock_1_year_ttl() {
        let mut w = TimingWheel::new(1_725_000_000); // 任意 epoch
        // 一年 TTL（MockClock 加速）：365 天 8 小时
        let year = 365 * 86_400 + 8 * 3600;
        let fired = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&fired);
        w.schedule(
            year,
            "ttl-1y",
            Some(Box::new(move || { c.fetch_add(1, AOrdering::Relaxed); })),
        );
        // 提前 1 秒不得触发
        w.advance(year - 1);
        assert_eq!(fired.load(AOrdering::Relaxed), 0, "TTL 未到不触发");
        assert_eq!(w.pending(), 1);
        // 第 year 秒触发
        w.advance(1);
        assert_eq!(fired.load(AOrdering::Relaxed), 1, "1 年 TTL 到点触发");
        assert_eq!(w.pending(), 0);

        // 混合多刻度精确性
        let mut w2 = TimingWheel::new(100);
        let hit = Arc::new(AtomicUsize::new(0));
        for delay in [1u64, 5, 59, 60, 61, 119, 3600, 3601, 86_400] {
            let c = Arc::clone(&hit);
            let now_at = delay; // 记录相对顺序：全部同秒推进后分别到期
            w2.schedule(delay, format!("d{delay}"), Some(Box::new(move || {
                let _ = now_at;
                c.fetch_add(1, AOrdering::Relaxed);
            })));
        }
        // 逐秒推进到 86401（覆盖各到期秒）
        for _ in 0..=86_401 {
            w2.tick();
        }
        assert_eq!(hit.load(AOrdering::Relaxed), 9, "各刻度任务均到点触发");
        assert_eq!(w2.pending(), 0);
    }

    /// 取消：未到期摘除（各层均可），已取消不触发。
    #[test]
    fn cancel_removes_from_any_level_and_no_fire() {
        let mut w = TimingWheel::new(0);
        let fired = Arc::new(AtomicUsize::new(0));
        let mk = || {
            let c = Arc::clone(&fired);
            Some(Box::new(move || { c.fetch_add(1, AOrdering::Relaxed); }) as Box<dyn FnOnce() + Send>)
        };
        let h_sec = w.schedule(10, "s", mk());
        let h_min = w.schedule(90, "m", mk());
        let h_hour = w.schedule(3600, "h", mk());
        let h_day = w.schedule(3 * 86_400, "d", mk());
        assert!(w.cancel(h_sec));
        assert!(w.cancel(h_min));
        assert!(w.cancel(h_hour));
        assert!(w.cancel(h_day));
        assert!(!w.cancel(h_sec), "重复取消 false");
        assert_eq!(w.pending(), 0);
        w.advance(3 * 86_400 + 10);
        assert_eq!(fired.load(AOrdering::Relaxed), 0, "取消任务不触发");
    }

    /// 检查点：每 10 分钟推进 → 文件含指针 + 未完成任务；重启恢复后重挂并到点触发。
    #[test]
    fn checkpoint_every_10min_and_restart_restores() {
        let path = tmp().join("checkpoint.json");
        let mut w = TimingWheel::new(1_000);
        w.enable_checkpoint(&path);
        let (_, a) = fired_counter();
        w.schedule(1_800, "job-a", Some(a)); // 30 分钟后到期
        let (_, b) = fired_counter();
        w.schedule(3 * 86_400, "job-b", Some(b));
        // 推进 600s（一次检查点周期）
        w.advance(600);
        assert!(path.exists(), "10 分钟应自动落检查点");
        assert_eq!(w.pending(), 2);
        let (now, tasks) = TimingWheel::load_checkpoint(&path).unwrap();
        assert_eq!(now, 1_600);
        assert_eq!(tasks.len(), 2, "未完成任务全部入检查点");
        assert!(tasks.iter().any(|t| t.kind == "job-a"));
        assert!(tasks.iter().any(|t| t.kind == "job-b"));
        // 再推进 600s → 指针推进但未完成任务保留
        w.advance(600);
        let (now2, _) = TimingWheel::load_checkpoint(&path).unwrap();
        assert_eq!(now2, 2_200);
        // ---- 模拟重启：重建轮，按检查点元数据重挂 action ----
        let mut w2 = TimingWheel::new(now2);
        w2.enable_checkpoint(&path);
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_a = Arc::clone(&fired);
        let fired_b = Arc::clone(&fired);
        let (overdue, rearms) = {
            let mut a_cb = Some(Box::new(move || { fired_a.fetch_add(1, AOrdering::Relaxed); }) as Box<dyn FnOnce() + Send>);
            let mut b_cb = Some(Box::new(move || { fired_b.fetch_add(1, AOrdering::Relaxed); }) as Box<dyn FnOnce() + Send>);
            w2.restore_from(&tasks, &mut |_exp, kind| match kind {
                "job-a" => a_cb.take(),
                "job-b" => b_cb.take(),
                _ => None,
            })
        };
        assert_eq!(overdue, 0);
        assert_eq!(rearms, 2, "重启后未完成任务重建");
        // job-a 剩余 1800 - 1200 = 600s；job-b 剩余 3 天 - 1200s
        w2.advance(600);
        assert_eq!(fired.load(AOrdering::Relaxed), 1, "job-a 重启后到点触发");
        assert_eq!(w2.pending(), 1);
        w2.advance(3 * 86_400 - 1_800);
        assert_eq!(fired.load(AOrdering::Relaxed), 2, "job-b 重启后到点触发");
        assert_eq!(w2.pending(), 0);
    }

    /// 关闭时落盘最终检查点（Drop）。
    #[test]
    fn drop_writes_final_checkpoint() {
        let path = tmp().join("cp2.json");
        {
            let mut w = TimingWheel::new(0);
            w.enable_checkpoint(&path);
            let (_, _a) = fired_counter();
            w.schedule(5_000, "pending-job", Some(_a));
            // drop 时自动落盘
        }
        let (now, tasks) = TimingWheel::load_checkpoint(&path).unwrap();
        assert_eq!(now, 0);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].kind, "pending-job");
    }

    /// 空轮大推进 O(1)（无任务 → 指针直接跳）。
    #[test]
    fn empty_wheel_advance_is_instant() {
        let mut w = TimingWheel::new(0);
        w.advance(1_000_000_000);
        assert_eq!(w.now(), 1_000_000_000);
        assert_eq!(w.pending(), 0);
    }
}
