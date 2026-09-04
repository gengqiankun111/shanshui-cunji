//! `backup` / `restore` 子命令：备份还原（步骤 14）。

use std::path::{Path, PathBuf};

use shanshui_cunji::config::Config;

/// 备份：打开引擎做一致性准备（刷 WAL + MemTable + 倒排）→ 打包数据目录为单个备份文件。
pub(crate) fn run_backup(config_path: &Path, backup_file: &Path) {
    let cfg = match Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };
    let data_dir = PathBuf::from(&cfg.storage.data_dir);

    // 打开引擎执行 prepare_backup：保证 WAL/MemTable/倒排内存全部落盘，磁盘态自包含
    let mut engine = match shanshui_cunji::engine::Engine::open(&data_dir, &cfg) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("❌ 打开引擎失败（备份前一致性准备需要）: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = engine.prepare_backup() {
        eprintln!("❌ 备份前一致性准备失败: {e}");
        std::process::exit(1);
    }
    drop(engine);

    match shanshui_cunji::backup::backup(&data_dir, backup_file) {
        Ok(rep) => {
            println!("✅ 备份完成: {}", backup_file.display());
            println!(
                "   {} 个文件，{} 字节（{:.0} ms）",
                rep.entry_count, rep.total_bytes, rep.elapsed_ms
            );
        }
        Err(e) => {
            eprintln!("❌ 备份失败: {e}");
            std::process::exit(1);
        }
    }
}

/// 还原：停止服务后执行——清空数据目录 → 校验魔数/版本/CRC → 解压全部文件。
pub(crate) fn run_restore(config_path: &Path, backup_file: &Path) {
    let cfg = match Config::load(config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ 配置加载失败: {e}");
            std::process::exit(1);
        }
    };
    let data_dir = PathBuf::from(&cfg.storage.data_dir);
    match shanshui_cunji::backup::restore(backup_file, &data_dir) {
        Ok(rep) => {
            println!(
                "✅ 还原完成: {} 个文件，{} 字节（{:.0} ms）",
                rep.entry_count, rep.total_bytes, rep.elapsed_ms
            );
            println!(
                "   数据目录: {}（重启 server 即可加载还原的数据）",
                data_dir.display()
            );
        }
        Err(e) => {
            eprintln!("❌ 还原失败: {e}");
            std::process::exit(1);
        }
    }
}
