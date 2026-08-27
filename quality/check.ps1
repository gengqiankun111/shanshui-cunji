# shanshui-cunji static check one-shot script (Windows / PowerShell)
# Usage: powershell -File quality/check.ps1
# Six steps: fmt -> clippy -> build -> test -> audit -> deny (quality_system_process.md section 2)

$ErrorActionPreference = "Stop"
$env:TMP = "D:\w64devkit\tmp"
$env:TEMP = "D:\w64devkit\tmp"
$env:PATH = "D:\w64devkit\bin;" + $env:PATH

Write-Host "== 1/6 cargo fmt --check =="
cargo fmt --all -- --check
if ($LASTEXITCODE -ne 0) { Write-Host "[FAIL] fmt"; exit 1 }

Write-Host "== 2/6 cargo clippy -D warnings =="
cargo clippy -- -D warnings
if ($LASTEXITCODE -ne 0) { Write-Host "[FAIL] clippy"; exit 1 }

Write-Host "== 3/6 cargo build =="
cargo build
if ($LASTEXITCODE -ne 0) { Write-Host "[FAIL] build"; exit 1 }

Write-Host "== 4/6 cargo test --lib =="
$ErrorActionPreference = "Continue"   # PS5.1: cargo stderr progress triggers NativeCommandError
cargo test --lib 2>&1 | Select-String -Pattern "test result"
$testExit = $LASTEXITCODE
$ErrorActionPreference = "Stop"
if ($testExit -ne 0) { Write-Host "[FAIL] test"; exit 1 }

Write-Host "== 5/6 cargo audit (local advisory db) =="
$env:PATH = "D:\rust-tools\bin;" + $env:PATH
$ErrorActionPreference = "Continue"   # PS5.1: cargo stderr (progress/log) triggers NativeCommandError
if (Get-Command cargo-audit -ErrorAction SilentlyContinue) {
    cargo audit --no-fetch *> $null
    $auditExit = $LASTEXITCODE
    if ($auditExit -ne 0) {
        Write-Host "[WARN] audit failed with local db, trying online update"
        cargo audit *> $null
        $auditExit = $LASTEXITCODE
    }
    $ErrorActionPreference = "Stop"
    if ($auditExit -ne 0) { Write-Host "[FAIL] audit"; exit 1 }
    Write-Host "[OK] audit: $auditExit (0 vulns)"
} else {
    $ErrorActionPreference = "Stop"
    Write-Host "[SKIP] cargo-audit not installed (cargo install cargo-audit)"
}

Write-Host "== 6/6 cargo deny check =="
$ErrorActionPreference = "Continue"
if (Get-Command cargo-deny -ErrorAction SilentlyContinue) {
    # advisories 由 cargo audit 覆盖；此处只查合规/重复依赖，避免联网 fetch 数据库
    cargo deny check bans licenses sources
    $denyExit = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    if ($denyExit -ne 0) { Write-Host "[FAIL] deny"; exit 1 }
} else {
    $ErrorActionPreference = "Stop"
    Write-Host "[SKIP] cargo-deny not installed (cargo install cargo-deny)"
}

Write-Host "[OK] static check chain passed"
