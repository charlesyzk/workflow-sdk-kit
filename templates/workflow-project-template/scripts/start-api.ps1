param([int]$Port = 8090)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

$projectRoot = Get-WorkflowProjectRoot
Assert-WorkflowEnv -ProjectRoot $projectRoot
$python = Get-WorkflowPython -ProjectRoot $projectRoot
Set-Location -LiteralPath $projectRoot

& $python -m uvicorn workflow_app.main:app --host 0.0.0.0 --port $Port
