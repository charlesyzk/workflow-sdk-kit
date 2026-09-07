$ErrorActionPreference = "Stop"

$body = @{
    workflow_type = "number_analysis"
    actor_id = "afternoon-tester"
    input = @{
        numbers = @(10, 20, 30, 40)
        multiplier = 1.5
        delay_seconds = 3
    }
} | ConvertTo-Json -Depth 5

$accepted = Invoke-RestMethod `
    -Method Post `
    -Uri "http://127.0.0.1:8090/api/v1/tasks" `
    -ContentType "application/json" `
    -Body $body

Write-Host "HTTP 202 accepted:" ($accepted | ConvertTo-Json -Compress)
$taskId = $accepted.task_id

do {
    Start-Sleep -Milliseconds 500
    $task = Invoke-RestMethod -Uri "http://127.0.0.1:8090/api/v1/tasks/$taskId"
    Write-Host "status=" $task.status "node=" $task.current_node
} while ($task.status -notin @("SUCCEEDED", "FAILED", "CANCELLED", "BLOCKED"))

$trace = Invoke-RestMethod -Uri "http://127.0.0.1:8090/api/v1/tasks/$taskId/trace"
Write-Host "Trace:"
$trace | ConvertTo-Json -Depth 8

if ($task.final_artifact_id) {
    $artifact = Invoke-RestMethod -Uri "http://127.0.0.1:8090/api/v1/artifacts/$($task.final_artifact_id)"
    Write-Host "Final artifact:"
    $artifact | ConvertTo-Json -Depth 8
}

