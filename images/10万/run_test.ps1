# 功能测试脚本（10 万条）：插入/主键/缓存/组合索引/倒排/分片/删除
# 用法: powershell -File run_test.ps1
$ErrorActionPreference = "Stop"
$exe = "D:\shanshui-cunji-target\release\shanshui-cunji.exe"
if (-not (Test-Path $exe)) { Write-Host "未找到 $exe，请先执行 cargo build --release"; exit 1 }
& $exe demo --scale 100000 --out $PSScriptRoot
Write-Host "完成：报告已生成 $PSScriptRoot\report.html"
