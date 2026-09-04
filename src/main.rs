//! shanshui-cunji 二进制入口：仅保留最小入口与子命令分发委托。
//! 参数解析与各子命令处理逻辑见 [`cli`] 模块。

mod cli;

fn main() {
    if let Err(e) = cli::run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}
