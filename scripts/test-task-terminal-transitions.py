"""验证真实任务系统在任务失败后的状态机行为。

脚本只从项目根目录 ``.env`` 读取凭据，不打印 API Key，也不会把凭据写入结果。
每次运行会创建两条带唯一幂等键的测试任务：第一条用于制造节点失败并探测非法
回退，第二条用于确认“新建任务重试”仍然可用。
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    config = dotenv_values(ROOT / ".env")
    base_url = str(config.get("EXECUTION_TASK_API_BASE_URL") or "").rstrip("/")
    api_key = str(config.get("EXECUTION_TASK_API_KEY") or "")
    caller_id = str(config.get("EXECUTION_TASK_CALLER_ID") or "workbench-service")
    if not base_url or not api_key:
        raise SystemExit(".env 缺少 EXECUTION_TASK_API_BASE_URL 或 EXECUTION_TASK_API_KEY")

    run_tag = f"terminal-probe-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    results: list[dict[str, Any]] = []

    with httpx.Client(
        base_url=base_url,
        headers={"X-API-Key": api_key, "X-Caller-Id": caller_id},
        timeout=20,
        # 生产域名应直接访问；避免本机 7897 代理改变 DNS、TLS 或响应内容。
        trust_env=False,
    ) as client:
        def call(label: str, method: str, path: str, body: dict[str, Any] | None = None) -> httpx.Response:
            response = client.request(method, path, json=body)
            try:
                response_body: Any = response.json()
            except ValueError:
                response_body = response.text[:2000]
            results.append({"label": label, "method": method, "path": path, "status": response.status_code, "body": response_body})
            return response

        created = call(
            "create_failure_probe",
            "POST",
            "/api/v1/execution/tasks",
            {"input": {"probe": run_tag}, "idempotentKey": run_tag, "executionMode": "RecordOnly"},
        )
        created.raise_for_status()
        task_id = created.json()["taskId"]

        call("task_running", "PUT", f"/api/v1/execution/tasks/{task_id}/status", {"status": "Running", "businessStatus": "Running", "output": {"probe": run_tag}})
        call("step_1_start", "POST", f"/api/v1/execution/tasks/{task_id}/steps/intent_recognition/start", {"sessionId": f"{run_tag}:intent", "stepInput": {"probe": True}})
        call("step_1_success", "PUT", f"/api/v1/execution/tasks/{task_id}/steps/intent_recognition/status", {"status": "Success", "output": {"validated": True}})
        call("step_2_start", "POST", f"/api/v1/execution/tasks/{task_id}/steps/query_rewrite/start", {"sessionId": f"{run_tag}:rewrite", "stepInput": {"probe": True}})
        call("step_2_failed", "PUT", f"/api/v1/execution/tasks/{task_id}/steps/query_rewrite/status", {"status": "Failed", "output": {"reason": "intentional terminal-state probe"}})

        # 先不主动修改任务状态，立即读取服务端状态，借此判断“步骤失败”是否会由
        # 远端状态机自动级联为“任务失败”。这是本测试最重要的观察点。
        call("read_task_after_step_failed", "GET", f"/api/v1/execution/tasks/{task_id}")
        # 使用完全相同的 sessionId/入参重放 start，可排除“新会话参数冲突”，只观察
        # Failed 步骤本身能否回到 Running。
        call("restart_failed_step_while_task_open", "POST", f"/api/v1/execution/tasks/{task_id}/steps/query_rewrite/start", {"sessionId": f"{run_tag}:rewrite", "stepInput": {"probe": True}})
        call("change_failed_step_while_task_open", "PUT", f"/api/v1/execution/tasks/{task_id}/steps/query_rewrite/status", {"status": "Success", "output": {"forced": True}})
        call("start_downstream_after_dependency_failed", "POST", f"/api/v1/execution/tasks/{task_id}/steps/confirm_rewrite/start", {"sessionId": f"{run_tag}:confirm-rewrite", "stepInput": {"probe": True}})
        call("task_failed", "PUT", f"/api/v1/execution/tasks/{task_id}/status", {"status": "Failed", "businessStatus": "Failed", "output": {"reason": "intentional terminal-state probe"}})

        # 以下三个请求专门验证原任务能否从失败终态回退。预期均被状态机拒绝，
        # 因而不会推进任何正常业务数据。
        call("failed_task_to_running", "PUT", f"/api/v1/execution/tasks/{task_id}/status", {"status": "Running", "businessStatus": "Retrying", "output": {}})
        call("failed_step_restart", "POST", f"/api/v1/execution/tasks/{task_id}/steps/query_rewrite/start", {"sessionId": f"{run_tag}:rewrite:retry", "stepInput": {"probe": True}})
        call("failed_step_to_success", "PUT", f"/api/v1/execution/tasks/{task_id}/steps/query_rewrite/status", {"status": "Success", "output": {"forced": True}})
        call("read_failed_task", "GET", f"/api/v1/execution/tasks/{task_id}")

        # SDK 当前采用的重试模型：为同一本地任务创建一个新的远端任务。
        retry_tag = f"{run_tag}-retry-1"
        call("create_retry_task", "POST", "/api/v1/execution/tasks", {"input": {"probe": run_tag, "retryOf": task_id}, "idempotentKey": retry_tag, "executionMode": "RecordOnly"})

        # 单独构造“成功节点之后被用户驳回”的非失败场景，验证旧方案是否能在同一
        # taskId 内把已完成节点改回 Running。这个探针不会把任务置为 Failed。
        rollback_tag = f"{run_tag}-rollback"
        rollback_created = call("rollback_create", "POST", "/api/v1/execution/tasks", {"input": {"probe": rollback_tag}, "idempotentKey": rollback_tag, "executionMode": "RecordOnly"})
        rollback_created.raise_for_status()
        rollback_task_id = rollback_created.json()["taskId"]
        call("rollback_task_running", "PUT", f"/api/v1/execution/tasks/{rollback_task_id}/status", {"status": "Running", "businessStatus": "Running", "output": {}})
        call("rollback_target_start", "POST", f"/api/v1/execution/tasks/{rollback_task_id}/steps/intent_recognition/start", {"sessionId": f"{rollback_tag}:intent", "stepInput": {"probe": True}})
        call("rollback_target_success", "PUT", f"/api/v1/execution/tasks/{rollback_task_id}/steps/intent_recognition/status", {"status": "Success", "output": {"round": 1}})
        call("rollback_task_waiting", "PUT", f"/api/v1/execution/tasks/{rollback_task_id}/status", {"status": "AwaitingConfirmation", "businessStatus": "WaitingUser", "output": {"at": "intent_recognition"}})
        call("rollback_task_resume", "PUT", f"/api/v1/execution/tasks/{rollback_task_id}/status", {"status": "Running", "businessStatus": "Rejected", "output": {"rollbackTo": "intent_recognition"}})
        call("rollback_success_step_to_running", "PUT", f"/api/v1/execution/tasks/{rollback_task_id}/steps/intent_recognition/status", {"status": "Running", "output": {"round": 2}})
        call("rollback_restart_same_session", "POST", f"/api/v1/execution/tasks/{rollback_task_id}/steps/intent_recognition/start", {"sessionId": f"{rollback_tag}:intent", "stepInput": {"probe": True}})
        call("rollback_restart_new_session", "POST", f"/api/v1/execution/tasks/{rollback_task_id}/steps/intent_recognition/start", {"sessionId": f"{rollback_tag}:intent:round-2", "stepInput": {"probe": True, "round": 2}})
        call("read_rollback_task", "GET", f"/api/v1/execution/tasks/{rollback_task_id}")

    output_path = ROOT / "integration-results" / f"task-terminal-probe-{run_tag}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps({"runTag": run_tag, "results": results}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(output_path))
    for item in results:
        print(f"{item['label']}: HTTP {item['status']}")


if __name__ == "__main__":
    main()
