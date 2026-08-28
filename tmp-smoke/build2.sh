#!/usr/bin/env bash
# 重构建 4 个 bench 版本（显式 vendor 路径覆盖用户级配置）
set -e
source /root/.cargo/env
cd /root/scc-bench
CFG="--config source.crates-io.replace-with=\"vendored-sources\" --config source.vendored-sources.directory=\"/root/scc-bench/vendor\" --config net.offline=true"
echo "=== gmi ==="
cargo build --release --bin shanshui-cunji-bench --target-dir target-gmi $CFG 2>&1 | tail -1
echo "=== gsys ==="
cargo build --release --no-default-features --bin shanshui-cunji-bench --target-dir target-gsys $CFG 2>&1 | tail -1
echo "=== mmi ==="
cargo build --release --target x86_64-unknown-linux-musl --bin shanshui-cunji-bench --target-dir target-mmi $CFG 2>&1 | tail -1
echo "=== msys ==="
cargo build --release --no-default-features --target x86_64-unknown-linux-musl --bin shanshui-cunji-bench --target-dir target-msys $CFG 2>&1 | tail -1
echo BUILD_ALL_OK
