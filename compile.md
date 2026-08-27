山水存迹数据库（shanshui-cunji）编译与部署指南
版本：v1.0
适用版本：shanshui-cunji >= v0.1（MVP）
关联文档：Readme.md | design.md | development.md

1. 文档定位与适用人群
本指南涵盖 shanshui-cunji 从源码到可运行产物的完整流程，包括本地开发编译、生产环境静态编译、交叉编译、Docker 镜像构建及常见问题排查。

角色	阅读重点
用户（想从源码编译使用）	第 2、3、4、6、9 章
开发者（想贡献代码）	第 2、3、8、9 章
运维人员（需部署到特定环境）	第 4、5、6、7、9 章
2. 前置依赖
2.1 Rust 工具链
bash
# 安装 Rust（如未安装）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 确认版本（shanshui-cunji 需要 Rust 1.70+）
rustc --version
cargo --version
2.2 各平台系统依赖
平台	依赖包	安装命令
Ubuntu / Debian	build-essential, pkg-config, cmake, musl-tools	sudo apt update && sudo apt install -y build-essential pkg-config cmake musl-tools
CentOS / RHEL / Fedora	gcc, gcc-c++, make, pkg-config, cmake, musl-gcc	sudo dnf install -y gcc gcc-c++ make pkgconfig cmake musl-gcc
Alpine Linux	gcc, musl-dev, make, cmake, pkgconfig	apk add gcc musl-dev make cmake pkgconfig
macOS	Xcode Command Line Tools	xcode-select --install
Windows	LLVM, MSVC Build Tools	通过 Visual Studio Installer 安装“使用 C++ 的桌面开发”工作负载
2.3 验证依赖
bash
# 检查必要工具是否就绪
cargo --version
gcc --version || clang --version
make --version
3. 本地开发编译（快速迭代）
3.1 标准编译
bash
# 克隆仓库
git clone https://github.com/your-org/shanshui-cunji.git
cd shanshui-cunji

# 调试模式编译（速度快，体积大，含调试符号）
cargo build

# 发布模式编译（优化后，体积小，适合性能测试）
cargo build --release
3.2 运行测试
bash
# 运行全部测试
cargo test

# 运行指定集成测试
cargo test --test integration_crud

# 运行指定单元测试
cargo test --lib -- memtable::tests::test_insert

# 仅编译测试（不执行）
cargo test --no-run
3.3 启用调试日志
bash
# 运行时输出 DEBUG 级别日志
RUST_LOG=debug cargo run -- server --config config.toml

# 仅输出特定模块日志
RUST_LOG=shanshui-cunji::engine=debug cargo run -- server

# 输出 TRACE 级别（最详细）
RUST_LOG=trace cargo run -- server
3.4 开发时常用命令速查
命令	说明
cargo check	快速检查编译错误（跳过代码生成）
cargo clippy	运行 Linter，检查代码质量
cargo fmt	自动格式化代码
cargo doc --open	生成并打开本地文档
4. 生产环境静态编译（musl）
4.1 为什么使用 musl？
shanshui-cunji 采用 完全静态链接 的方式编译，生成不依赖任何外部 .so 动态库的二进制文件。这带来了：

✅ 零依赖部署：直接复制到任意 Linux 系统即可运行

✅ 极致轻量的 Docker 镜像：可运行于 scratch 空镜像

✅ 避免 glibc 版本兼容性问题

4.2 编译步骤
bash
# 1. 添加 MUSL 编译目标
rustup target add x86_64-unknown-linux-musl

# 2. 编译（完全静态链接）
cargo build --release --target x86_64-unknown-linux-musl

# 3. 产物位置
ls -lh target/x86_64-unknown-linux-musl/release/shanshui-cunji
4.3 验证静态链接
bash
# 使用 file 命令查看文件类型
file target/x86_64-unknown-linux-musl/release/shanshui-cunji
# 应输出包含 "statically linked" 字样

# 使用 ldd 检查动态库依赖（若显示 "not a dynamic executable" 则正确）
ldd target/x86_64-unknown-linux-musl/release/shanshui-cunji
# 期望输出: "statically linked" 或 "not a dynamic executable"
4.4 体积优化（可选）
在 Cargo.toml 的 [profile.release] 中添加以下配置，可显著减小二进制体积：

toml
[profile.release]
lto = "fat"          # 链接时优化，提升性能并减小体积
codegen-units = 1    # 提高优化效果（增加编译时间）
strip = "symbols"    # 移除调试符号（可减小 30%~50% 体积）
优化后重新编译：

bash
cargo build --release --target x86_64-unknown-linux-musl
4.5 使用 cross 工具（推荐）
对于复杂的项目或交叉编译场景，cross 工具能自动处理环境问题：

bash
# 安装 cross
cargo install cross --git https://github.com/cross-rs/cross

# 使用 cross 编译（命令与 cargo 完全一致）
cross build --release --target x86_64-unknown-linux-musl
5. 交叉编译（ARM / Nova OS / 边缘设备）
5.1 目标平台
目标平台	Target 名称	适用场景
ARM64（Linux）	aarch64-unknown-linux-musl	边缘设备、Nova OS、树莓派 4/5
ARMv7（Linux）	armv7-unknown-linux-musleabihf	老旧 ARM 设备、树莓派 2/3
ARM64（Linux，动态链接）	aarch64-unknown-linux-gnu	使用 glibc 的 ARM 系统
5.2 方案一：使用 cross 工具（推荐）
bash
# 添加目标
rustup target add aarch64-unknown-linux-musl

# 编译（cross 自动拉取预配置的 Docker 环境）
cross build --release --target aarch64-unknown-linux-musl

# 产物位置
ls -lh target/aarch64-unknown-linux-musl/release/shanshui-cunji
5.3 方案二：手动配置交叉编译工具链
适用于需要精细控制或无法使用 Docker 的 CI/CD 环境：

1. 安装 ARM 交叉编译工具链

bash
# Ubuntu / Debian
sudo apt install -y gcc-aarch64-linux-gnu binutils-aarch64-linux-gnu

# 或使用 musl 工具链
sudo apt install -y gcc-aarch64-linux-musl
2. 创建 .cargo/config.toml

toml
[target.aarch64-unknown-linux-musl]
linker = "aarch64-linux-musl-gcc"

[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"
3. 执行编译

bash
cargo build --release --target aarch64-unknown-linux-musl
5.4 验证交叉编译产物
bash
# 确认架构正确
file target/aarch64-unknown-linux-musl/release/shanshui-cunji
# 应输出包含 "ELF 64-bit LSB executable, ARM aarch64" 字样
6. Docker 镜像构建
6.1 推荐 Dockerfile（多阶段构建）
dockerfile
# ========== 阶段 1: 构建（Builder）==========
FROM rust:1.82-slim AS builder

# 安装 MUSL 目标
RUN rustup target add x86_64-unknown-linux-musl

# 安装系统依赖（如有原生 C 依赖）
# RUN apt update && apt install -y pkg-config musl-tools

WORKDIR /app

# ---- 【关键优化】利用 Docker 层缓存 ----
# 单独复制依赖文件，只要 Cargo.toml 不变就不重新下载依赖
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    rm -rf src

# ---- 复制源码并构建 ----
COPY src ./src
COPY config.toml ./
# 强制重新编译项目（依赖层已缓存）
RUN touch src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl

# ========== 阶段 2: 运行（Runtime）==========
# 使用空镜像，极致轻量且安全
FROM scratch

# 复制二进制文件
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/shanshui-cunji /usr/local/bin/shanshui-cunji

# 复制配置文件（可选）
COPY --from=builder /app/config.toml /etc/shanshui-cunji/config.toml

# 声明端口
EXPOSE 8080 9090

# 设置启动命令
ENTRYPOINT ["/usr/local/bin/shanshui-cunji"]
CMD ["server", "--config", "/etc/shanshui-cunji/config.toml"]
6.2 构建 Docker 镜像
bash
# 构建镜像
docker build -t shanshui-cunji:latest -f Dockerfile .

# 查看镜像大小
docker images | grep shanshui-cunji
# 期望：最终镜像约 20~50MB

# 运行容器
docker run -d \
  --name shanshui-cunji \
  -p 8080:8080 \
  -v ./data:/var/lib/shanshui-cunji \
  shanshui-cunji:latest

# 验证运行
curl http://localhost:8080/status
6.3 多架构 Docker 镜像（buildx）
如需同时支持 x86_64 和 ARM64：

bash
# 创建 buildx 构建器
docker buildx create --name multiarch --use

# 构建并推送多架构镜像
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag your-registry/shanshui-cunji:latest \
  --push .
6.4 Docker Compose 快速启动
yaml
# docker-compose.yml
version: '3.8'

services:
  shanshui-cunji:
    image: shanshui-cunji:latest
    container_name: shanshui-cunji
    ports:
      - "8080:8080"
      - "9090:9090"
    volumes:
      - ./data:/var/lib/shanshui-cunji
      - ./config.toml:/etc/shanshui-cunji/config.toml
    environment:
      - RUST_LOG=info
    restart: unless-stopped
bash
docker-compose up -d
7. CI/CD 集成建议
7.1 GitHub Actions 工作流示例
yaml
# .github/workflows/build.yml
name: Build and Test

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64-unknown-linux-musl, aarch64-unknown-linux-musl]
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
          target: ${{ matrix.target }}
          override: true

      - name: Build
        uses: actions-rs/cargo@v1
        with:
          command: build
          args: --release --target ${{ matrix.target }}

      - name: Upload Artifact
        uses: actions/upload-artifact@v4
        with:
          name: shanshui-cunji-${{ matrix.target }}
          path: target/${{ matrix.target }}/release/shanshui-cunji
7.2 加速编译的建议
工具	说明	启用方式
sccache	分布式编译缓存	cargo install sccache，设置 RUSTC_WRAPPER=sccache
cargo-chef	优化 Docker 层缓存	在 Dockerfile 中使用 cargo chef 精确计算依赖哈希
增量编译	本地开发时启用	在 Cargo.toml 中设置 [profile.dev] incremental = true
8. 产物检查清单
编译完成后，建议执行以下检查：

bash
# 1. 检查文件是否存在
ls -lh target/*/release/shanshui-cunji

# 2. 确认架构
file target/x86_64-unknown-linux-musl/release/shanshui-cunji

# 3. 确认静态链接（musl 目标）
ldd target/x86_64-unknown-linux-musl/release/shanshui-cunji
# 期望: "statically linked" 或 "not a dynamic executable"

# 4. 检查版本信息（如已注入版本号）
./target/release/shanshui-cunji --version

# 5. 检查配置文件是否存在
cat config.toml

# 6. （可选）检查符号表大小
nm --size-sort target/release/shanshui-cunji | tail -20
9. 常见问题与解决方案（FAQ）
Q1：编译时报错 "failed to run custom build command for openssl-sys"
原因：openssl-sys crate 需要系统安装 OpenSSL 开发库，或静态链接时找不到 MUSL 版本的 OpenSSL。

解决方案：

bash
# 方案一：安装系统 OpenSSL 开发库
sudo apt install -y libssl-dev pkg-config  # Ubuntu/Debian
sudo dnf install -y openssl-devel          # Fedora/CentOS

# 方案二：使用 Rustls 替代 OpenSSL（推荐）
# 在 Cargo.toml 中禁用默认特性，启用 rustls
# [dependencies]
# reqwest = { version = "0.11", default-features = false, features = ["rustls-tls"] }
Q2：cross 拉取镜像很慢或卡住怎么办？
bash
# 手动预拉取镜像
docker pull ghcr.io/cross-rs/x86_64-unknown-linux-musl:latest

# 使用国内镜像源（如阿里云）
export CROSS_DOCKER_IMAGE=registry.cn-hangzhou.aliyuncs.com/rust-cross/x86_64-unknown-linux-musl

# 或直接使用 cargo 本地编译（见 4.2）
Q3：在 ARM 环境下运行时提示 "cannot execute binary file: Exec format error"
原因：编译的架构与运行平台不匹配（如用 x86_64 二进制跑在 ARM 上）。

解决方案：重新用正确的 target 编译：

bash
# 查看当前平台架构
uname -m

# 如果是 aarch64，使用对应 target 编译
cargo build --release --target aarch64-unknown-linux-musl
Q4：Docker 镜像构建时 cargo build 反复下载依赖（缓存失效）
原因：Cargo.toml 或 Cargo.lock 发生变化导致缓存层失效。

解决方案：在 Dockerfile 中使用 cargo chef 优化：

dockerfile
# 安装 cargo-chef
FROM rust:1.82-slim AS planner
RUN cargo install cargo-chef
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# 构建时使用 chef 计算精确缓存
FROM rust:1.82-slim AS builder
RUN cargo install cargo-chef
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release
Q5：编译时间太长，有没有加速方法？
方案	命令	预期提升
使用 sccache	RUSTC_WRAPPER=sccache cargo build	二次编译提速 50%~70%
减少 LTO 级别（开发时）	[profile.dev] lto = "off"	本地编译提速 30%
使用 mold 链接器	RUSTFLAGS="-C linker=mold" cargo build	链接阶段提速 2~3 倍
仅检查（跳过代码生成）	cargo check	替代完整编译用于语法检查
Q6：编译后二进制文件太大（> 100MB）
解决方案：

检查是否启用了 strip = "symbols"（见 4.4）

检查是否开启了 lto = "fat"（体积优化）

检查是否包含了不必要的依赖（使用 cargo tree 查看依赖树）

Q7：Windows 上编译时提示 "link.exe not found"
原因：未安装 MSVC 编译工具链。

解决方案：

安装 Visual Studio Build Tools

安装时勾选“使用 C++ 的桌面开发”

在 Visual Studio 开发者命令提示符中运行 cargo build

Q8：如何注入 Git Commit Hash 作为版本号？
在 build.rs 中添加：

rust
// build.rs
use std::process::Command;

fn main() {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .unwrap();
    let git_hash = String::from_utf8(output.stdout).unwrap();
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
}
在代码中使用：env!("GIT_HASH")。

10. 快速命令索引
场景	命令
本地调试编译	cargo build
发布编译（本地）	cargo build --release
静态编译（musl）	cargo build --release --target x86_64-unknown-linux-musl
交叉编译 ARM	cross build --release --target aarch64-unknown-linux-musl
运行测试	cargo test
构建 Docker 镜像	docker build -t shanshui-cunji:latest .
运行容器	docker run -p 8080:8080 shanshui-cunji:latest
验证静态链接	ldd target/.../shanshui-cunji
11. 相关文档
文档	说明
Readme.md	项目概览与快速开始
design.md	架构设计与技术决策
development.md	开发指南与模块详解
文档状态：定稿 ✓