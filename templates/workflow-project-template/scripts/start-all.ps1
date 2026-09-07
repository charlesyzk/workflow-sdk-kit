param(
    [int]$Port = 8090,
    [switch]$SkipRedis
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

$projectRoot = Get-WorkflowProjectRoot
Assert-WorkflowEnv -ProjectRoot $projectRoot
$python = Get-WorkflowPython -ProjectRoot $projectRoot
$arq = Get-WorkflowArq -ProjectRoot $projectRoot
$runtimeDirectory = Join-Path $projectRoot ".runtime"
$logDirectory = Join-Path $projectRoot "logs"
$manifestPath = Join-Path $runtimeDirectory "processes.json"

if (Test-Path -LiteralPath $manifestPath) {
    throw "A process manifest already exists. Run .\scripts\stop-all.ps1 before starting again."
}

New-Item -ItemType Directory -Force -Path $runtimeDirectory, $logDirectory | Out-Null
Set-Location -LiteralPath $projectRoot

if (-not $SkipRedis) {
    # Redis 数据通过 Compose volume 保留；start/stop 不会删除 volume。
    docker compose up -d redis
    if ($LASTEXITCODE -ne 0) { throw "Failed to start Redis with Docker Compose." }

    $redisReady = $false
    foreach ($attempt in 1..20) {
        $ping = docker compose exec -T redis redis-cli ping 2>$null
        if ($LASTEXITCODE -eq 0 -and $ping -match "PONG") {
            $redisReady = $true
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $redisReady) { throw "Redis did not become ready within 20 seconds." }
}

# 在启动常驻进程前执行迁移，避免 API 已开始接收任务但数据库表尚未准备好。
& $python -m workflow_app.migrate
if ($LASTEXITCODE -ne 0) { throw "Database migration failed." }

function Start-WorkflowManagedProcess {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Executable,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $stdoutPath = Join-Path $logDirectory "$Name.stdout.log"
    $stderrPath = Join-Path $logDirectory "$Name.stderr.log"
    $process = Start-Process `
        -FilePath $Executable `
        -ArgumentList $Arguments `
        -WorkingDirectory $projectRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru

    return [PSCustomObject]@{
        name = $Name
        pid = $process.Id
        executable = (Resolve-Path -LiteralPath $Executable).Path
        stdout = $stdoutPath
        stderr = $stderrPath
    }
}

$processes = @()
try {
    $processes += Start-WorkflowManagedProcess `
        -Name "api" `
        -Executable $python `
        -Arguments @("-m", "uvicorn", "workflow_app.main:app", "--host", "0.0.0.0", "--port", "$Port")
    $processes += Start-WorkflowManagedProcess `
        -Name "workflow-worker" `
        -Executable $arq `
        -Arguments @("workflow_app.worker.WorkflowWorkerSettings")
    $processes += Start-WorkflowManagedProcess `
        -Name "sync-worker" `
        -Executable $arq `
        -Arguments @("workflow_app.worker.ExecutionSyncWorkerSettings")

    [PSCustomObject]@{
        created_at = (Get-Date).ToString("o")
        project_root = $projectRoot
        processes = $processes
    } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $manifestPath -Encoding UTF8
}
catch {
    # 如果第三个进程启动失败，回收本次已经启动的精确 PID，避免留下半套服务。
    foreach ($item in $processes) {
        Stop-Process -Id $item.pid -ErrorAction SilentlyContinue
    }
    throw
}

try {
    Start-Sleep -Seconds 2

    # Start-Process 成功只说明操作系统创建了进程。Worker 仍可能因为 Redis 地址、
    # 配置或导入错误立即退出，所以在报告成功前再次检查三个精确 PID。
    $stoppedProcesses = @(
        $processes | Where-Object {
            $null -eq (Get-Process -Id $_.pid -ErrorAction SilentlyContinue)
        }
    )
    if ($stoppedProcesses.Count -gt 0) {
        $names = ($stoppedProcesses | ForEach-Object { $_.name }) -join ", "
        throw "Processes exited during startup: $names. Inspect files under $logDirectory."
    }

    $health = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health"
    $ready = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/ready"
    Write-Host "All processes started."
    Write-Host "Health:" ($health | ConvertTo-Json -Compress)
    Write-Host "Ready:" ($ready | ConvertTo-Json -Compress)
    Write-Host "API docs: http://127.0.0.1:$Port/docs"
    Write-Host "Logs: $logDirectory"
    Write-Host "Stop: .\scripts\stop-all.ps1"
}
catch {
    foreach ($item in $processes) {
        Stop-Process -Id $item.pid -ErrorAction SilentlyContinue
    }
    Remove-Item -LiteralPath $manifestPath -ErrorAction SilentlyContinue
    if (-not $SkipRedis) {
        docker compose stop redis | Out-Null
    }
    throw
}
