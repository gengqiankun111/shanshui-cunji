$ErrorActionPreference = "Stop"
$exe = "D:\novosdb-target\release\shanshui-cunji.exe"
if (-not (Test-Path $exe)) { Write-Host "未找到 $exe，请先执行 cargo build --release"; exit 1 }
& $exe demo --gen-only --scale 1000000 --out $PSScriptRoot
Write-Host "完成：数据已写入 $PSScriptRoot\data.jsonl"
