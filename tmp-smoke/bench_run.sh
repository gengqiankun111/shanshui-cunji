#!/usr/bin/env bash
# 分配器高并发压测：glibc/musl × system/mimalloc
set -e
cd /root/scc-bench
R=/root/scc-bench/bench-results.txt
: > "$R"
for BIN in \
  target-gsys/release/shanshui-cunji-bench:glibc-system \
  target-gmi/release/shanshui-cunji-bench:glibc-mimalloc \
  target-msys/x86_64-unknown-linux-musl/release/shanshui-cunji-bench:musl-system \
  target-mmi/x86_64-unknown-linux-musl/release/shanshui-cunji-bench:musl-mimalloc
do
  B="${BIN%%:*}"
  NAME="${BIN##*:}"
  echo "=== $NAME ===" >> "$R"
  for T in 1 2 4; do
    "$B" --threads "$T" --ops 400000 2>&1 | grep bench >> "$R"
  done
done
cat "$R"
