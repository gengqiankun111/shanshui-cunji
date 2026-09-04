//! `server` 子命令：启动 HTTP-JSON 服务（development 步骤 15；默认子命令）。

use std::path::{Path, PathBuf};

use tracing::info;

use super::{load_config, VERSION};

pub(crate) fn run_server(config_path: &Path) {
    let cfg = load_config(config_path);
    let data_dir = PathBuf::from(&cfg.storage.data_dir);
    let mut engine = match shanshui_cunji::engine::Engine::open(&data_dir, &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ 打开引擎失败: {e}");
            std::process::exit(1);
        }
    };
    info!(
        "shanshui-cunji {VERSION} 启动: data_dir={}",
        data_dir.display()
    );
    let broadcast = Some(shanshui_cunji::join::JoinBroadcast {
        enabled: cfg.join.broadcast_enabled,
        threshold: cfg.join.broadcast_threshold,
    });
    if let Err(e) = shanshui_cunji::server::serve(&mut engine, &cfg.server.listen_addr, broadcast) {
        eprintln!("❌ 服务异常退出: {e}");
        std::process::exit(1);
    }
}
