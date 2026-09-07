param([switch]$KeepRedis)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

$projectRoot = Get-WorkflowProjectRoot
$manifestPath = Join-Path $projectRoot ".runtime\processes.json"

if (Test-Path -LiteralPath $manifestPath -PathType Leaf) {
    $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
    foreach ($item in $manifest.processes) {
        $process = Get-Process -Id ([int]$item.pid) -ErrorAction SilentlyContinue
        if ($null -eq $process) {
            Write-Host "$($item.name) is already stopped."
            continue
        }

        # PID 可能被操作系统复用；只有可执行文件仍与启动清单完全一致时才终止，
        # 防止陈旧 PID 文件误伤其他进程。
        $actualPath = $process.Path
        if (-not $actualPath -or $actualPath -ne [string]$item.executable) {
            Write-Warning "Skipped PID $($item.pid): executable no longer matches $($item.name)."
            continue
        }
        Stop-Process -Id $process.Id
        Write-Host "Stopped $($item.name) (PID $($process.Id))."
    }
    Remove-Item -LiteralPath $manifestPath
}
else {
    Write-Host "No managed process manifest found."
}

if (-not $KeepRedis) {
    Set-Location -LiteralPath $projectRoot
    docker compose stop redis
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Redis could not be stopped; check Docker Compose manually."
    }
}
