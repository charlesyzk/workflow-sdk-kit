$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

$projectRoot = Get-WorkflowProjectRoot
$python = Get-WorkflowPython -ProjectRoot $projectRoot
Set-Location -LiteralPath $projectRoot

& $python -m pytest
if ($LASTEXITCODE -ne 0) { throw "Template tests failed." }

& $python -m compileall -q src
if ($LASTEXITCODE -ne 0) { throw "Template source compilation failed." }
