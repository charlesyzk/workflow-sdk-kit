param(
    [string]$OutputDirectory = "dist",
    [switch]$SkipZip
)

$ErrorActionPreference = "Stop"
$kitRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$templateSource = Join-Path $kitRoot "templates\workflow-project-template"
$sdkSource = Join-Path $kitRoot "sdk"

if (-not (Test-Path -LiteralPath (Join-Path $templateSource "pyproject.toml") -PathType Leaf)) {
    throw "Template source is incomplete: $templateSource"
}
if (-not (Test-Path -LiteralPath (Join-Path $sdkSource "pyproject.toml") -PathType Leaf)) {
    throw "SDK source is incomplete: $sdkSource"
}

# OutputDirectory 允许使用相对路径，但最终目标必须位于当前 kit 根目录内。构建会
# 清理同名旧产物，因此在删除前先规范化并验证绝对路径，避免参数错误扩大范围。
$resolvedOutputRoot = if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    [System.IO.Path]::GetFullPath($OutputDirectory)
}
else {
    [System.IO.Path]::GetFullPath((Join-Path $kitRoot $OutputDirectory))
}
$kitPrefix = $kitRoot.TrimEnd('\') + '\'
if (-not $resolvedOutputRoot.StartsWith($kitPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "OutputDirectory must stay inside the kit root: $kitRoot"
}

$projectOutput = Join-Path $resolvedOutputRoot "workflow-project-template"
$zipOutput = Join-Path $resolvedOutputRoot "workflow-project-template.zip"
$projectPrefix = $projectOutput.TrimEnd('\') + '\'

New-Item -ItemType Directory -Force -Path $resolvedOutputRoot | Out-Null
if (Test-Path -LiteralPath $projectOutput) {
    $checkedProjectOutput = (Resolve-Path -LiteralPath $projectOutput).Path
    if ($checkedProjectOutput -ne $projectOutput -or -not $checkedProjectOutput.StartsWith($kitPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to clean unexpected template output path: $checkedProjectOutput"
    }
    Remove-Item -LiteralPath $checkedProjectOutput -Recurse -Force
}
if (Test-Path -LiteralPath $zipOutput) {
    $checkedZipOutput = (Resolve-Path -LiteralPath $zipOutput).Path
    if (-not $checkedZipOutput.StartsWith($kitPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to replace unexpected ZIP path: $checkedZipOutput"
    }
    Remove-Item -LiteralPath $checkedZipOutput -Force
}

New-Item -ItemType Directory -Force -Path $projectOutput | Out-Null
Get-ChildItem -LiteralPath $templateSource -Force | ForEach-Object {
    Copy-Item -LiteralPath $_.FullName -Destination $projectOutput -Recurse -Force
}

# 真实联调和协议级 Mock 由 SDK 维护仓库统一维护，打包时注入模板，避免在模板
# 源码目录再维护一份副本。使用者可以在远端不可用时验证除远端 HTTP 之外的全部
# 生产组件，也可以在取得有效 Key 后原样切换到真实模式。
$integrationHelpers = @(
    "__init__.py",
    "run-real-integration.py",
    "mock_execution_task_server.py"
)
foreach ($helper in $integrationHelpers) {
    Copy-Item `
        -LiteralPath (Join-Path $kitRoot "scripts\$helper") `
        -Destination (Join-Path $projectOutput "scripts\$helper") `
        -Force
}

# 将当前 SDK 作为完整本地包放入 vendor。业务模板通过 setup.ps1 先安装该目录，
# 因而交付物在无内部 PyPI 的环境也能安装。构建时同步而不是人工维护重复源码。
$vendorSdk = Join-Path $projectOutput "vendor\obei-workflow-sdk"
New-Item -ItemType Directory -Force -Path $vendorSdk | Out-Null
Get-ChildItem -LiteralPath $sdkSource -Force | ForEach-Object {
    Copy-Item -LiteralPath $_.FullName -Destination $vendorSdk -Recurse -Force
}

# 把仍然有效的 SDK 文档放进交付模板。模板自己的 README 是唯一入门入口，这些
# 文件作为深入参考保留，不复制已经废弃或重复的旧文档。
$sdkDocsOutput = Join-Path $projectOutput "docs\sdk"
New-Item -ItemType Directory -Force -Path $sdkDocsOutput | Out-Null
$sdkDocuments = @(
    "CURRENT_STATUS.md",
    "TEMPLATE_PROJECT.md",
    "SDK_FEATURES.md",
    "BUILD_FIRST_WORKFLOW.md",
    "SDK_USAGE.md",
    "TIDB_REQUIRED_INDEXES.sql"
)
foreach ($document in $sdkDocuments) {
    Copy-Item `
        -LiteralPath (Join-Path $kitRoot "docs\$document") `
        -Destination (Join-Path $sdkDocsOutput $document) `
        -Force
}

# BUILD_FIRST_WORKFLOW 在 SDK 仓库中链接 examples/simple_workflow。交付模板已经
# 用 workflow_app 提供同一实现，因此只在生成副本中把链接改到模板自身文件，
# 保持仓库文档和 ZIP 内文档都可点击，而不复制一份冗余示例项目。
$buildTutorialPath = Join-Path $sdkDocsOutput "BUILD_FIRST_WORKFLOW.md"
$buildTutorial = Get-Content -LiteralPath $buildTutorialPath -Raw
$tutorialLinkMap = [ordered]@{
    "../examples/simple_workflow/src/simple_app/workflow.py" = "../../src/workflow_app/workflows/four_stage.py"
    "../examples/simple_workflow/src/simple_app/container.py" = "../../src/workflow_app/container.py"
    "../examples/simple_workflow/src/simple_app/main.py" = "../../src/workflow_app/main.py"
    "../examples/simple_workflow/src/simple_app/worker.py" = "../../src/workflow_app/worker.py"
    "../examples/simple_workflow/src/simple_app/migrate.py" = "../../src/workflow_app/migrate.py"
    "../examples/simple_workflow/tests/test_simple_workflow.py" = "../../tests/test_four_stage_workflow.py"
    "../examples/simple_workflow/.env.example" = "../../.env.example"
    "../examples/simple_workflow/simple_llm_workflow.registration.json" = "../../generated/registration/simple_llm_workflow.registration.json"
    "../examples/simple_workflow" = "../.."
}
foreach ($sourceLink in $tutorialLinkMap.Keys) {
    $buildTutorial = $buildTutorial.Replace($sourceLink, $tutorialLinkMap[$sourceLink])
}
Set-Content -LiteralPath $buildTutorialPath -Value $buildTutorial -Encoding UTF8

# SDK_USAGE 在仓库中位于 docs/，因此它到根 .env.example 只上跳一级；复制到模板
# docs/sdk/ 后需要上跳两级。只改生成副本，不改变仓库内正确链接。
$sdkUsagePath = Join-Path $sdkDocsOutput "SDK_USAGE.md"
$sdkUsage = (Get-Content -LiteralPath $sdkUsagePath -Raw).Replace(
    "../.env.example",
    "../../.env.example"
)
Set-Content -LiteralPath $sdkUsagePath -Value $sdkUsage -Encoding UTF8

# 模板源码 README 位于仓库 templates/ 下，链接到仓库根 docs；生成后这些文档被
# 放在模板自己的 docs/sdk，因此同步修正交付副本中的四个深入阅读链接。
$templateReadmePath = Join-Path $projectOutput "README.md"
$templateReadme = Get-Content -LiteralPath $templateReadmePath -Raw
foreach ($document in @("SDK_FEATURES.md", "BUILD_FIRST_WORKFLOW.md", "SDK_USAGE.md", "TIDB_REQUIRED_INDEXES.sql")) {
    $templateReadme = $templateReadme.Replace(
        "../../docs/$document",
        "docs/sdk/$document"
    )
}
Set-Content -LiteralPath $templateReadmePath -Value $templateReadme -Encoding UTF8

# editable install 和测试会产生缓存/egg-info。交付物只保留源文件与元数据，减小
# ZIP 并避免把构建机器的绝对路径带给使用者。
$generatedDirectories = Get-ChildItem -LiteralPath $projectOutput -Directory -Recurse -Force |
    Where-Object { $_.Name -eq "__pycache__" -or $_.Name -eq ".pytest_cache" -or $_.Name -like "*.egg-info" } |
    Sort-Object { $_.FullName.Length } -Descending
foreach ($directory in $generatedDirectories) {
    $checkedPath = [System.IO.Path]::GetFullPath($directory.FullName)
    if (-not $checkedPath.StartsWith($projectPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove generated directory outside template output: $checkedPath"
    }
    Remove-Item -LiteralPath $checkedPath -Recurse -Force
}
Get-ChildItem -LiteralPath $projectOutput -File -Recurse -Force |
    Where-Object { $_.Extension -in ".pyc", ".pyo" } |
    ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force }

$sdkVersionLine = Get-Content -LiteralPath (Join-Path $sdkSource "pyproject.toml") |
    Where-Object { $_ -match '^version\s*=\s*"' } |
    Select-Object -First 1
$sdkVersion = if ($sdkVersionLine -match '"([^"]+)"') { $Matches[1] } else { "unknown" }
[PSCustomObject]@{
    template = "obei-workflow-project-template"
    sdk_version = $sdkVersion
    built_at = (Get-Date).ToUniversalTime().ToString("o")
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $projectOutput "BUILD_INFO.json") -Encoding UTF8

# 绝不把根目录真实 .env 打进模板；同时扫描常见 API Key 形态作为第二道保护。
$secretEnvFiles = Get-ChildItem -LiteralPath $projectOutput -File -Recurse -Force |
    Where-Object { $_.Name -eq ".env" }
if ($secretEnvFiles) {
    throw "Generated template unexpectedly contains a .env file."
}
$secretPattern = 'sk-[A-Za-z0-9_-]{16,}'
$secretMatches = Get-ChildItem -LiteralPath $projectOutput -File -Recurse -Force |
    Select-String -Pattern $secretPattern -ErrorAction SilentlyContinue
if ($secretMatches) {
    throw "Generated template appears to contain an API key; packaging aborted."
}

if (-not $SkipZip) {
    # ZipFile 会包含 .env.example、.gitignore 等点文件；压缩内容直接以模板根目录
    # 为 ZIP 根，使用者解压到任意新目录即可运行。
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [System.IO.Compression.ZipFile]::CreateFromDirectory(
        $projectOutput,
        $zipOutput,
        [System.IO.Compression.CompressionLevel]::Optimal,
        $false
    )
}

$fileCount = (Get-ChildItem -LiteralPath $projectOutput -File -Recurse -Force).Count
Write-Host "Template folder: $projectOutput"
if (-not $SkipZip) { Write-Host "Template ZIP:    $zipOutput" }
Write-Host "Files packaged:  $fileCount"
Write-Host "SDK version:     $sdkVersion"
