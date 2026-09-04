//! 服务端配置：运行模式与对外监听地址（design 9.8）。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// 运行模式（design 9.8）："standalone"（默认）/ "cluster"。
    pub mode: String,
    pub listen_addr: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: "standalone".into(),
            listen_addr: "0.0.0.0:8080".into(),
        }
    }
}