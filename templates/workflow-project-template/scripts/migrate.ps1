$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

$projectRoot = Get-WorkflowProjectRoot
Assert-WorkflowEnv -ProjectRoot $projectRoot
$python = Get-WorkflowPython -ProjectRoot $projectRoot
Set-Location -LiteralPath $projectRoot

& $python -m workflow_app.migrate
if ($LASTEXITCODE -ne 0) { throw "Database migration failed." }
