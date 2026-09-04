//! 13.6 拓扑并行：Kahn 拓扑分层（层内无依赖可并行；环/非法依赖 → Error::Config）。

use std::collections::VecDeque;

use crate::error::{Error, Result};

/// 13.6：Kahn 拓扑分层。`deps[i]` = 步骤 i 依赖的步骤索引集合（前置）。
/// 返回拓扑层序列（层内互相无依赖、可并行）；环 / 非法索引 → Error::Config（网关 400）。
pub(crate) fn topo_layers(n: usize, deps: &[Vec<usize>]) -> Result<Vec<Vec<usize>>> {
    if deps.len() != n {
        return Err(Error::Config(format!(
            "依赖声明数量 {} ≠ 步骤数 {n}",
            deps.len()
        )));
    }
    for (i, d) in deps.iter().enumerate() {
        for &p in d {
            if p >= n || p == i {
                return Err(Error::Config(format!("步骤 {i} 依赖索引非法: {p}")));
            }
        }
    }
    let mut indeg = vec![0usize; n];
    for (i, d) in deps.iter().enumerate() {
        indeg[i] = d.len();
    }
    let mut adj = vec![Vec::new(); n];
    for (i, d) in deps.iter().enumerate() {
        for &p in d {
            adj[p].push(i);
        }
    }
    let mut queue: VecDeque<usize> = (0..n).filter(|&i| indeg[i] == 0).collect();
    let mut layers = Vec::new();
    let mut visited = 0;
    while !queue.is_empty() {
        let cur: Vec<usize> = queue.drain(..).collect();
        visited += cur.len();
        for &i in &cur {
            for &j in &adj[i] {
                indeg[j] -= 1;
                if indeg[j] == 0 {
                    queue.push_back(j);
                }
            }
        }
        layers.push(cur);
    }
    if visited != n {
        return Err(Error::Config("SAGA 步骤依赖构成环".into()));
    }
    Ok(layers)
}
