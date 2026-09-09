$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

$projectRoot = Get-WorkflowProjectRoot
Assert-WorkflowEnv -ProjectRoot $projectRoot
$arq = Get-WorkflowArq -ProjectRoot $projectRoot
Set-Location -LiteralPath $projectRoot

& $arq workflow_app.worker.ExecutionSyncWorkerSettings
