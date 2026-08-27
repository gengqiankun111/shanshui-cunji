# 山水存迹数据库 · 静态检查一键脚本（Windows / PowerShell）
# 用法：powershell -File quality/check.ps1
# 六步：fmt → clippy → build → test → audit → deny（对应 quality_system_process.md 第 2 节）

$ErrorActionPreference = "Stop"
$env:TMP = "D:\w64devkit\tmp"
$env:TEMP = "D:\w64devkit\tmp"
$env:PATH = "D:\w64devkit\bin;" + $env:PATH

Write-Host "== 1/4 cargo fmt --check =="
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { Write-Host "❌ fmt 未通过，先运行: cargo fmt --all"; exit 1 }

Write-Host "== 2/4 cargo clippy -D warnings =="
cargo clippy -- -D warnings
if ($LASTEXITCODE -ne 0) { Write-Host "❌ clippy 有警告/错误"; exit 1 }

Write-Host "== 3/4 cargo build =="
cargo build
if ($LASTEXITCODE -ne 0) { Write-Host "❌ 构建失败"; exit 1 }

Write-Host "== 4/4 cargo test --lib =="
$ErrorActionPreference = "Continue"   # PS5.1: cargo 的 stderr 进度会触发 NativeCommandError
cargo test --lib 2>&1 | Select-String -Pattern "test result"
$testExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
if ($testExit -ne 0) { Write-Host "❌ 测试失败"; exit 1 }

Write-Host "== 5/6 cargo audit（依赖漏洞）=="
$env:PATH = "D:\rust-tools\bin;" + $env:PATH
if (Get-Command cargo-audit -ErrorAction SilentlyContinue) {
    cargo audit
    if ($LASTEXITCODE -ne 0) { Write-Host "❌ audit 发现漏洞/警告"; exit 1 }
} else {
    Write-Host "⚠️  cargo-audit 未安装，跳过（cargo install cargo-audit）"
}

Write-Host "== 6/6 cargo deny check（许可证/重复依赖）=="
if (Get-Command cargo-deny -ErrorAction SilentlyContinue) {
    cargo deny check
    if ($LASTEXITCODE -ne 0) { Write-Host "❌ deny 校验未通过"; exit 1 }
} else {
    Write-Host "⚠️  cargo-deny 未安装，跳过（cargo install cargo-deny）"
}

Write-Host "✅ 静态检查链全部通过"
