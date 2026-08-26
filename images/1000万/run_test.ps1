$ErrorActionPreference = "Stop"
$exe = "D:\novosdb-target\release\novosdb.exe"
if (-not (Test-Path $exe)) { Write-Host "未找到 $exe，请先执行 cargo build --release"; exit 1 }
& $exe demo --scale 10000000 --out $PSScriptRoot
Write-Host "完成：报告已生成 $PSScriptRoot\report.html"
