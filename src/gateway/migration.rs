//! 无损扩容协议（design 9.1.1）：阶段一 双写（Shadow Writes）→ 阶段二 数据追平
//! （Delta Catch-up）→ 阶段三 原子切换（Atomic Switch）；失败可回滚（abort）。

use super::{Gateway, ShardEndpoint};
use crate::error::{Error, Result};
use crate::meta::NodeInfo;
use crate::reshard::{compute_moved_vshards, Migration};

impl<E: ShardEndpoint> Gateway<E> {
    /// 阶段一：开始迁移（新节点加入，暂不接收读流量）。返回需迁移的虚拟分片数。
    /// 计算新旧节点集合下归属变化的虚拟分片，进入双写（Shadow Writes）。
    pub fn begin_migration(
        &mut self,
        new_node: &str,
        new_addr: &str,
        new_role: &str,
    ) -> Result<usize> {
        if self.migration.is_some() {
            return Err(Error::Cluster(
                "已有迁移进行中，请先 commit 或 abort".into(),
            ));
        }
        let old_nodes = self.meta.node_ids();
        if old_nodes.contains(&new_node.to_string()) {
            return Err(Error::Cluster(format!("节点已存在: {new_node}")));
        }
        let mut new_nodes = old_nodes.clone();
        new_nodes.push(new_node.to_string());
        let moved = compute_moved_vshards(&old_nodes, &new_nodes, self.meta.virtual_shards());
        // 新节点接入端点（双写 / 追平），但**不注册进元数据中心**（路由暂不变）
        self.endpoint.add_node(new_node, Some(new_addr))?;
        let n = moved.len();
        self.migration = Some(Migration::new(
            new_node.to_string(),
            new_addr.to_string(),
            new_role.to_string(),
            moved,
        ));
        Ok(n)
    }

    /// 阶段二：数据追平（Delta Catch-up）。全量扫描老节点数据，把属于迁移分片的
    /// 文档拷贝到新节点（物理形态为 SST 拷贝 + WAL 增量，此处为逻辑全量拷贝，语义等价）。
    /// 返回拷贝条数。追平期间的新写入由双写兜底。
    pub fn catch_up(&mut self) -> Result<usize> {
        let Some(m) = &self.migration else {
            return Err(Error::Cluster("未处于迁移状态".into()));
        };
        let moved = m.moved_vshards.clone();
        let new_node = m.new_node.clone();
        let old_nodes: Vec<NodeInfo> = self.meta.broadcast_targets().into_iter().cloned().collect();
        let mut copied = 0usize;
        for old in &old_nodes {
            if old.node_id == new_node {
                continue;
            }
            let rows = self.endpoint.scan_all(&old.node_id)?;
            for (docid, data) in rows {
                let vs = self.virtual_shard_of(docid);
                if !moved.contains(&vs) {
                    continue;
                }
                // 从文档 JSON 重新派生倒排词条（与写入路径 extract_terms 一致）
                let terms = match serde_json::from_str::<serde_json::Value>(&data) {
                    Ok(v) => crate::server::extract_terms(&v),
                    Err(_) => Vec::new(),
                };
                self.endpoint.put(&new_node, docid, &data, &terms)?;
                copied += 1;
            }
        }
        Ok(copied)
    }

    /// 阶段三：原子切换（Atomic Switch）。将新节点注册进元数据中心（路由映射切换），
    /// 关闭双写。返回切换的虚拟分片数。
    pub fn commit_migration(&mut self) -> Result<usize> {
        let Some(m) = &self.migration else {
            return Err(Error::Cluster("未处于迁移状态".into()));
        };
        let new_node = m.new_node.clone();
        let new_addr = m.new_addr.clone();
        let new_role = m.new_role.clone();
        let moved = m.moved_vshards.len();
        self.meta.register(&new_node, &new_addr, &new_role)?;
        self.migration = None; // 双写关闭，路由已切至新节点
        Ok(moved)
    }

    /// 回滚预案：Node-B 启动失败 → 放弃迁移（新节点不注册，路由不变，旧数据完好）。
    pub fn abort_migration(&mut self) {
        self.migration = None;
    }

    /// 当前迁移状态（测试 / 监控）。
    pub fn migration(&self) -> Option<&Migration> {
        self.migration.as_ref()
    }
}
