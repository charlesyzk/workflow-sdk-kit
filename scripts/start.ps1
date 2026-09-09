$ErrorActionPreference = "Stop"
$kitRoot = Split-Path -Parent $PSScriptRoot
Set-Location $kitRoot
if (-not (Test-Path -LiteralPath ".env")) {
    throw "Missing .env. Copy .env.example to .env and configure the real EXECUTION_TASK_API_KEY first."
}
$databaseLine = Get-Content -LiteralPath ".env" |
    Where-Object { $_ -match '^WORKFLOW_DATABASE_URL=' } |
    Select-Object -Last 1

# .env.example 使用 Compose 服务名 mysql，此时显式启用 local-db profile；真实
# TiDB URL 使用外部主机，默认 profile 会跳过本地 MySQL，缩短启动并避免误连。
if ($databaseLine -match '@mysql:') {
    docker compose --profile local-db up --build
} else {
    docker compose up --build
}
