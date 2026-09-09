param(
    [string]$PythonCommand = "python",
    [switch]$SkipPipUpgrade
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$sdkRoot = Join-Path $projectRoot "vendor\obei-workflow-sdk"
$venvRoot = Join-Path $projectRoot ".venv"
$venvPython = Join-Path $venvRoot "Scripts\python.exe"

if (-not (Test-Path -LiteralPath (Join-Path $sdkRoot "pyproject.toml") -PathType Leaf)) {
    throw "Vendored SDK is missing. Use the generated template ZIP/folder, not templates/workflow-project-template source directly."
}

if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
    Write-Host "Creating isolated Python environment at $venvRoot"
    & $PythonCommand -m venv $venvRoot
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create .venv with '$PythonCommand'. Python 3.11+ is required."
    }
}

if (-not $SkipPipUpgrade) {
    & $venvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) { throw "Failed to upgrade pip." }
}

# 先安装本地 SDK，再安装业务模板。业务 pyproject 中的固定版本约束会由这个本地
# 包满足，因此全程不需要从公网查找 obei-workflow-sdk。
& $venvPython -m pip install -e $sdkRoot
if ($LASTEXITCODE -ne 0) { throw "Failed to install vendored SDK." }

& $venvPython -m pip install -e "${projectRoot}[dev]"
if ($LASTEXITCODE -ne 0) { throw "Failed to install workflow project." }

$envPath = Join-Path $projectRoot ".env"
if (-not (Test-Path -LiteralPath $envPath)) {
    Copy-Item -LiteralPath (Join-Path $projectRoot ".env.example") -Destination $envPath
    Write-Host "Created .env from .env.example. Fill LLM and task-system credentials before real integration."
}

Write-Host "Template setup completed."
Write-Host "Next: .\scripts\test.ps1, then edit src\workflow_app\workflows\four_stage.py"
