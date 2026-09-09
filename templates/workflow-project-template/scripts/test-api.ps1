param(
    [string]$BaseUrl = "http://127.0.0.1:8090",
    [string]$Question = "请用两三句话说明工作流为什么需要持久化执行状态。"
)

$ErrorActionPreference = "Stop"

$body = @{
    workflow_type = "simple_llm_workflow"
    actor_id = "template-api-test"
    input = @{
        question = $Question
    }
} | ConvertTo-Json -Depth 5

$accepted = Invoke-RestMethod `
    -Method Post `
    -Uri "$BaseUrl/api/v1/tasks" `
    -ContentType "application/json" `
    -Body $body
Write-Host "Accepted:" ($accepted | ConvertTo-Json -Compress)

$taskId = $accepted.task_id
do {
    Start-Sleep -Milliseconds 500
    $task = Invoke-RestMethod -Uri "$BaseUrl/api/v1/tasks/$taskId"
    Write-Host "status=$($task.status) node=$($task.current_node)"
} while ($task.status -notin @("SUCCEEDED", "FAILED", "CANCELLED", "BLOCKED"))

$trace = Invoke-RestMethod -Uri "$BaseUrl/api/v1/tasks/$taskId/trace"
$llm = Invoke-RestMethod -Uri "$BaseUrl/api/v1/tasks/$taskId/llm-invocations"
Write-Host "Trace:"
$trace | ConvertTo-Json -Depth 10
Write-Host "LLM invocations:"
$llm | ConvertTo-Json -Depth 10

if ($task.status -ne "SUCCEEDED") {
    throw "Workflow did not succeed. Inspect logs and trace above."
}

if ($task.final_artifact_id) {
    $artifact = Invoke-RestMethod -Uri "$BaseUrl/api/v1/artifacts/$($task.final_artifact_id)"
    Write-Host "Final artifact:"
    $artifact | ConvertTo-Json -Depth 10
}
