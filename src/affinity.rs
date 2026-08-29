//! CPU 绑核（Ex-7.2，design_extension v0.5 第 12.2）：网络/计算/IO 三池物理核分区。
//!
//! - `plan_partition`：纯函数规划三池核列表（显式配置优先，空则自动分区——
//!   network=核 0 起、compute=中间段、io=尾部段；1 核机器退化为 no-op）；
//! - `bind_current`：把**当前线程**绑定到指定核（core_affinity crate，跨 Windows/Linux/macOS；
//!   绑定失败仅记录不致命——部署可用 taskset 兜底）；
//! - 跳过超线程虚拟核：由部署方在配置中显式填写物理核编号列表（自动分区按逻辑核）。
//!
//! 作用点：server 主线程（network）、Compaction 并行线程（compute）、组提交后台线程（io）。

use crate::config::model::AffinityConfig;

/// 三池核分区。
#[derive(Debug, Clone, Default)]
pub struct CpuPartition {
    pub network: Vec<usize>,
    pub compute: Vec<usize>,
    pub io: Vec<usize>,
}

/// 逻辑核总数（当前可用并行度，至少 1）。
pub fn logical_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1)
}

/// 规划三池分区：显式列表优先；空则自动分区。
pub fn plan_partition(cfg: &AffinityConfig) -> CpuPartition {
    if !cfg.enabled {
        return CpuPartition::default();
    }
    let n = logical_cores();
    let all: Vec<usize> = (0..n).collect();
    let network = if cfg.network_cores.is_empty() {
        vec![0]
    } else {
        cfg.network_cores.clone()
    };
    let compute = if cfg.compute_cores.is_empty() {
        if n >= 4 {
            all[1..n / 2].to_vec()
        } else {
            vec![]
        }
    } else {
        cfg.compute_cores.clone()
    };
    let io = if cfg.io_cores.is_empty() {
        if n >= 4 {
            all[n / 2..].to_vec()
        } else {
            vec![]
        }
    } else {
        cfg.io_cores.clone()
    };
    CpuPartition {
        network,
        compute,
        io,
    }
}

/// 绑当前线程到指定核列表（首可用核）。无核/单核/绑定失败返回 false（调用方忽略即可）。
pub fn bind_current(cores: &[usize]) -> bool {
    if cores.is_empty() {
        return false;
    }
    let ids = core_affinity::get_core_ids().unwrap_or_default();
    if ids.is_empty() {
        return false;
    }
    for idx in cores {
        if let Some(id) = ids.get(*idx) {
            if core_affinity::set_for_current(*id) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_auto_for_quad_core() {
        // 模拟 ≥4 核自动分区：network=[0]，compute=中部，io=尾部，互不重叠
        let cfg = AffinityConfig {
            network_cores: vec![0],
            compute_cores: vec![1],
            io_cores: vec![2],
            ..Default::default()
        };
        let p = plan_partition(&cfg);
        assert_eq!(p.network, vec![0]);
        assert_eq!(p.compute, vec![1]);
        assert_eq!(p.io, vec![2]);
        // 三池无交集（本机核数充足时）
        let p2 = plan_partition(&AffinityConfig::default());
        if logical_cores() >= 4 {
            assert!(p2.network.iter().all(|c| !p2.compute.contains(c)));
            assert!(p2.compute.iter().all(|c| !p2.io.contains(c)));
        }
    }

    #[test]
    fn disabled_plan_is_empty() {
        let cfg = AffinityConfig {
            enabled: false,
            ..Default::default()
        };
        let p = plan_partition(&cfg);
        assert!(p.network.is_empty() && p.compute.is_empty() && p.io.is_empty());
    }

    #[test]
    fn explicit_cores_override_auto() {
        let cfg = AffinityConfig {
            compute_cores: vec![3, 5],
            io_cores: vec![7],
            ..Default::default()
        };
        let p = plan_partition(&cfg);
        assert_eq!(p.compute, vec![3, 5]);
        assert_eq!(p.io, vec![7]);
        assert_eq!(p.network, vec![0], "network 未显式时自动核 0");
    }
}
