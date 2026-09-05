<#
 宽表 SQL 对比（MySQL ↔ cjserver）一键基准脚本 —— 与《宽表SQL性能对比-测试步骤存档.md》配套。

 用法示例（在仓库根执行）：
   # 110 万全流程（MySQL 3316 + SCC 3317，先建库建表/装数/起服/跑探针/合表）
   powershell -ExecutionPolicy Bypass -File user_guide\宽表SQL对比-基准.ps1 -Rows 1098342 -Build

   # 10 万，两端已装好数据只跑探针+合表
   powershell -ExecutionPolicy Bypass -File user_guide\宽表SQL对比-基准.ps1 -Rows 100000 `
     -SkipMySqlLoad -SkipSccReload -SkipMySqlStart -SkipSccStart

 参数：
   -Rows            100000 / 1098342（结果目录后缀 100k / 1100k）
   -Build           先 cargo build release（cjserver + rr-conformance）
   -MyPort / -SccPort
   -SkipMySqlStart  不尝试启动 MySQL（已运行则跳过探测）
   -SkipSccStart    不尝试启动 SCC
   -SkipMySqlLoad   跳过 MySQL 侧装载
   -SkipSccReload   跳过 SCC 侧重建+装载（数据已装好时用）
   -SkipProbes      跳过 37 探针与合表（仅准备数据）
   -ResultsBase     结果目录前缀，默认 results-sqlrun（gitignore）
 依赖：python3（tmp/tmp_wide_load.py、tmp_compare_sqlrun.py）、
       MySQL 实例/配置、cjserver 与 rr-conformance release 产物（见存档 §2/§7）。
#>

param(
    [ValidateSet(100000, 1098342)]
    [int]$Rows = 100000,
    [switch]$Build,
    [int]$MyPort = 3316,
    [int]$SccPort = 3317,
    [switch]$SkipMySqlStart,
    [switch]$SkipSccStart,
    [switch]$SkipMySqlLoad,
    [switch]$SkipSccReload,
    [switch]$SkipProbes,
    [string]$ResultsBase = "results\results-sqlrun"
)

$ErrorActionPreference = 'Stop'
$Repo = Split-Path -Parent $PSScriptRoot
$Label = if ($Rows -eq 1098342) { '1100k' } else { '100k' }

# ---- 资产路径（存档 §7；按需覆盖） ----
$MyIni  = "$Repo\tmp\my-wide-3316.ini"
$MyData = 'D:\shanshui-data\mysql-wide-2g'
$SccData = 'D:\shanshui-data\db-wide-scc'
$SccCfg  = "$Repo\tmp\tmp-cfg-wide-2g.toml"
$Mysqld = 'C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqld.exe'
$MyUrl  = "mysql://root@127.0.0.1:$MyPort/wide"
$SccUrl = "mysql://root@127.0.0.1:$SccPort"

function Find-Exe {
    param([string[]]$Names)
    $dirs = @()
    if ($env:CARGO_TARGET_DIR) { $dirs += "$env:CARGO_TARGET_DIR\release" }
    $dirs += "$Repo\target\release"; $dirs += 'D:\shanshui-cunji-target\release'
    foreach ($d in $dirs) { foreach ($n in $Names) { $p = Join-Path $d $n; if (Test-Path $p) { return $p } } }
    return $null
}
$RR = Find-Exe @('rr-conformance.exe')
$Scc = Find-Exe @('cjserver.exe', 'shanshui-cunji-mysql-server.exe')
if (-not $RR) { throw "未找到 rr-conformance.exe，先 -Build 或构建 release" }
if (-not $Scc) { throw "未找到 cjserver.exe，先 -Build 或构建 release" }

function Test-PortOpen {
    param([int]$Port)
    $c = New-Object System.Net.Sockets.TcpClient
    try { $iar = $c.BeginConnect('127.0.0.1', $Port, $null, $null); $ok = $iar.AsyncWaitHandle.WaitOne(800); if ($ok) { $c.EndConnect($iar) }; return $c.Connected } catch { return $false } finally { $c.Dispose() }
}
function Wait-Port {
    param([int]$Port, [int]$Sec = 60)
    for ($i = 0; $i -lt $Sec * 2; $i++) { if (Test-PortOpen $Port) { return $true }; Start-Sleep -Milliseconds 500 }
    return $false
}
function Stop-PortOwner {
    param([int]$Port)
    if (-not (Test-PortOpen $Port)) { return }
    $p = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($p) { Stop-Process -Id $p.OwningProcess -Force -ErrorAction SilentlyContinue; Start-Sleep -Seconds 2 }
}

Write-Host "== 宽表 SQL 对比（$Label 行，MySQL:$MyPort ↔ SCC:$SccPort）=="
if ($Build) {
    Write-Host '-- 构建 release --'
    cargo build --release --bin cjserver --target-dir (Split-Path (Split-Path $Scc -Parent) -Parent)
    cargo build --release --manifest-path "$Repo\rr-conformance\Cargo.toml" --target-dir (Split-Path (Split-Path $RR -Parent) -Parent)
    $RR = Find-Exe @('rr-conformance.exe'); $Scc = Find-Exe @('cjserver.exe', 'shanshui-cunji-mysql-server.exe')
}

if (-not $SkipMySqlStart -and -not (Test-PortOpen $MyPort)) {
    if (-not (Test-Path $Mysqld)) { throw "MySQL 未运行且找不到 mysqld：$Mysqld（请 -SkipMySqlStart 手动起）" }
    Write-Host "-- 启动 MySQL（$MyIni）--"
    Start-Process -FilePath $Mysqld -ArgumentList "--defaults-file=$MyIni" -WindowStyle Hidden
    if (-not (Wait-Port $MyPort)) { throw "MySQL $MyPort 启动超时" }
}
if (-not $SkipSccStart -and -not (Test-PortOpen $SccPort)) {
    Write-Host "-- 启动 SCC cjserver（$SccData / $SccPort）--"
    if (-not (Test-Path $SccCfg)) { throw "缺少 SCC 配置：$SccCfg" }
    Start-Process -FilePath $Scc -ArgumentList "--data-dir", $SccData, "--config", $SccCfg, "--bind", "127.0.0.1:$SccPort", "--watchdog-secs", "3600" -WindowStyle Hidden
    if (-not (Wait-Port $SccPort)) { throw "SCC $SccPort 启动超时" }
}

if (-not $SkipMySqlLoad) {
    Write-Host "-- MySQL 装载 $Rows 行（ddl + load）--"
    python "$Repo\tmp\tmp_wide_load.py" --mode ddl  --port $MyPort --user root
    python "$Repo\tmp\tmp_wide_load.py" --mode load --port $MyPort --user root --rows $Rows --procs 4
}
if (-not $SkipSccReload) {
    Write-Host "-- SCC 重建 + 装载 $Rows 行（documents 表）--"
    Stop-PortOwner $SccPort
    if (Test-Path $SccData) { Remove-Item $SccData -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $SccData | Out-Null
    Start-Process -FilePath $Scc -ArgumentList "--data-dir", $SccData, "--config", $SccCfg, "--bind", "127.0.0.1:$SccPort", "--watchdog-secs", "3600" -WindowStyle Hidden
    if (-not (Wait-Port $SccPort)) { throw "SCC 重建后启动超时" }
    & $RR --wide-load --url $SccUrl --table documents --rows $Rows --procs 4
    if ($LASTEXITCODE -ne 0) { throw "SCC wide-load 失败" }
}

if (-not $SkipProbes) {
    $MyOut  = "$ResultsBase-mysql-$Label"
    $SccOut = "$ResultsBase-scc-$Label"
    $CmpOut = "$ResultsBase-compare-$Label"
    Write-Host "-- 37 探针 MySQL → $MyOut --"
    & $RR --sql-run --url $MyUrl --table t --out $MyOut
    if ($LASTEXITCODE -ne 0) { throw "MySQL sql-run 失败" }
    Write-Host "-- 37 探针 SCC → $SccOut --"
    & $RR --sql-run --url $SccUrl --table documents --out $SccOut
    if ($LASTEXITCODE -ne 0) { throw "SCC sql-run 失败" }
    New-Item -ItemType Directory -Force -Path $CmpOut | Out-Null
    Write-Host "-- 合表对比 → $CmpOut\summary.md --"
    python "$Repo\tmp\tmp_compare_sqlrun.py" "$MyOut\summary.md" "$SccOut\summary.md" "$CmpOut\summary.md"
    if ($LASTEXITCODE -eq 0) { Write-Host "完成：$CmpOut\summary.md（另见两侧 $MyOut / $SccOut）" }
}
Write-Host '== 结束 =='
