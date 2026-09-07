$ErrorActionPreference = "Stop"

function Get-WorkflowProjectRoot {
    # common.ps1 位于 <project>/scripts；Resolve-Path 会返回规范绝对路径，避免脚本
    # 依赖调用者当前所在目录。
    return (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
}

function Get-WorkflowPython {
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)

    $pythonPath = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
    if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
        throw "Missing .venv. Run .\scripts\setup.ps1 first."
    }
    return $pythonPath
}

function Get-WorkflowArq {
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)

    $arqPath = Join-Path $ProjectRoot ".venv\Scripts\arq.exe"
    if (-not (Test-Path -LiteralPath $arqPath -PathType Leaf)) {
        throw "Missing ARQ executable. Run .\scripts\setup.ps1 first."
    }
    return $arqPath
}

function Assert-WorkflowEnv {
    param([Parameter(Mandatory = $true)][string]$ProjectRoot)

    $envPath = Join-Path $ProjectRoot ".env"
    if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
        throw "Missing .env. Copy .env.example to .env and configure it first."
    }
}
