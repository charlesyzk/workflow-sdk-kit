param(
    [string]$WorkflowType = "simple_llm_workflow"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common.ps1")

$projectRoot = Get-WorkflowProjectRoot
Assert-WorkflowEnv -ProjectRoot $projectRoot
$python = Get-WorkflowPython -ProjectRoot $projectRoot
$outputDirectory = Join-Path $projectRoot "generated\registration"
$outputPath = Join-Path $outputDirectory "$WorkflowType.registration.json"

New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
Set-Location -LiteralPath $projectRoot

& $python -m obei_workflow_sdk.export_definition `
    workflow_app.container:get_runtime `
    $WorkflowType `
    --output $outputPath
if ($LASTEXITCODE -ne 0) { throw "Registration JSON export failed." }

Write-Host "Generated registration definition: $outputPath"
Get-Content -LiteralPath $outputPath
