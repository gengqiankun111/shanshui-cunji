# ========== 阶段 1: 构建（Builder）==========
# 使用 slim 镜像作为基础（不用 alpine：过程宏依赖在 alpine 工具链下可能构建失败）
FROM rust:1.98-slim AS builder

# 安装 MUSL 目标（静态链接，零动态库依赖）
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /app

# 【关键优化】单独复制依赖文件，利用 Docker 层缓存：
# Cargo.toml / Cargo.lock 不变时，不会重新下载和编译依赖
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    rm -rf src

# 复制真正的源代码并构建（依赖层已缓存）
COPY src ./src
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl

# ========== 阶段 2: 运行（Runtime）==========
# scratch 空镜像：静态二进制零依赖，极致轻量且安全
FROM scratch

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/shanshui-cunji /shanshui-cunji
COPY config.toml /config.toml

EXPOSE 8080

ENTRYPOINT ["/shanshui-cunji", "--config", "/config.toml"]
