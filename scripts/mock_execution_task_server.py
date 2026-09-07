"""Protocol-level in-memory replacement for the temporarily unavailable task system.

The mock is intentionally stricter than a simple success stub. It authenticates the two required
headers, enforces idempotent task creation, validates the four registered step codes and their
dependencies, checks task/step state transitions, and exposes a GET endpoint for final assertions.
It never imports SDK internals, so passing this integration proves that the SDK speaks the external
HTTP contract instead of succeeding through a shared in-process shortcut.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Response


EXPECTED_API_KEY = os.environ.get(
    "MOCK_EXECUTION_TASK_API_KEY", "mock-execution-task-key"
)
ALLOWED_EXECUTION_MODES = {"RecordOnly", "Sync", "Async", "Scheduled"}
STEP_DEPENDENCIES = {
    "validate_input": [],
    "prepare_context": ["validate_input"],
    "generate_draft": ["prepare_context"],
    "polish_answer": ["generate_draft"],
}

app = FastAPI(title="Mock Execution Task System", version="1.0")
tasks: dict[str, dict[str, Any]] = {}
idempotency: dict[str, tuple[str, str]] = {}


def _authenticate(
    x_api_key: str | None,
    x_caller_id: str | None,
) -> None:
    """Apply the same mandatory header boundary documented by the real task system."""

    if not x_api_key:
        raise HTTPException(status_code=401, detail="missing X-API-Key")
    if x_api_key != EXPECTED_API_KEY:
        raise HTTPException(status_code=401, detail="invalid or revoked API key")
    if not x_caller_id:
        raise HTTPException(status_code=401, detail="missing X-Caller-Id")


def _task(task_id: str) -> dict[str, Any]:
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="task not found")
    return tasks[task_id]


def _payload_hash(body: dict[str, Any]) -> str:
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/execution/tasks")
def create_task(
    body: dict[str, Any],
    response: Response,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    x_caller_id: str | None = Header(default=None, alias="X-Caller-Id"),
) -> dict[str, Any]:
    """Create once with 201, or replay the same idempotent payload with 200."""

    _authenticate(x_api_key, x_caller_id)
    input_payload = body.get("input")
    idempotent_key = body.get("idempotentKey")
    execution_mode = body.get("executionMode")
    if not isinstance(input_payload, dict):
        raise HTTPException(status_code=422, detail="input must be an object")
    if not isinstance(idempotent_key, str) or not idempotent_key:
        raise HTTPException(status_code=422, detail="idempotentKey is required")
    if execution_mode not in ALLOWED_EXECUTION_MODES:
        raise HTTPException(status_code=422, detail="unsupported executionMode")

    digest = _payload_hash(body)
    previous = idempotency.get(idempotent_key)
    if previous is not None:
        previous_digest, previous_task_id = previous
        if previous_digest != digest:
            raise HTTPException(status_code=409, detail="idempotent payload conflict")
        response.status_code = 200
        previous_task = tasks[previous_task_id]
        return {
            "taskId": previous_task_id,
            "taskCode": previous_task["taskCode"],
            "status": previous_task["status"],
        }

    task_id = str(uuid4())
    task_code = str(input_payload.get("workflowType") or "simple_llm_workflow")
    tasks[task_id] = {
        "taskId": task_id,
        "taskCode": task_code,
        "status": "Pending",
        "businessStatus": "Queued",
        "progress": 0,
        "callerId": x_caller_id,
        "executionMode": execution_mode,
        "input": input_payload,
        "steps": {
            code: {
                "stepCode": code,
                "status": "Pending",
                "dependsOn": dependencies,
                "sessionId": None,
            }
            for code, dependencies in STEP_DEPENDENCIES.items()
        },
    }
    idempotency[idempotent_key] = (digest, task_id)
    response.status_code = 201
    return {"taskId": task_id, "taskCode": task_code, "status": "Pending"}


@app.put("/api/v1/execution/tasks/{task_id}/status")
def update_task_status(
    task_id: str,
    body: dict[str, Any],
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    x_caller_id: str | None = Header(default=None, alias="X-Caller-Id"),
) -> dict[str, Any]:
    _authenticate(x_api_key, x_caller_id)
    task = _task(task_id)
    requested = body.get("status")
    if requested not in {"Pending", "Running", "AwaitingConfirmation", "Success", "Failed"}:
        raise HTTPException(status_code=422, detail="unsupported task status")
    if task["status"] in {"Success", "Failed"} and requested != task["status"]:
        raise HTTPException(status_code=409, detail="terminal task cannot transition")
    if requested == "Success" and any(
        step["status"] != "Success" for step in task["steps"].values()
    ):
        raise HTTPException(status_code=409, detail="all steps must succeed first")
    task["status"] = requested
    task["businessStatus"] = body.get("businessStatus")
    task["output"] = body.get("output")
    if requested == "Success":
        task["progress"] = 100
    return {"taskId": task_id, "status": requested}


@app.post("/api/v1/execution/tasks/{task_id}/steps/{step_code}/start")
def start_step(
    task_id: str,
    step_code: str,
    body: dict[str, Any],
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    x_caller_id: str | None = Header(default=None, alias="X-Caller-Id"),
) -> dict[str, Any]:
    _authenticate(x_api_key, x_caller_id)
    task = _task(task_id)
    if step_code not in STEP_DEPENDENCIES:
        raise HTTPException(status_code=404, detail="step is not registered")
    step = task["steps"][step_code]
    for dependency in step["dependsOn"]:
        if task["steps"][dependency]["status"] != "Success":
            raise HTTPException(status_code=409, detail=f"dependency not complete: {dependency}")
    if not body.get("sessionId"):
        raise HTTPException(status_code=422, detail="sessionId is required")
    if step["status"] not in {"Pending", "Running"}:
        raise HTTPException(status_code=409, detail="step cannot start from current status")
    step["status"] = "Running"
    step["sessionId"] = body["sessionId"]
    step["stepInput"] = body.get("stepInput")
    return {"taskId": task_id, "stepCode": step_code, "status": "Running"}


@app.put("/api/v1/execution/tasks/{task_id}/steps/{step_code}/status")
def update_step_status(
    task_id: str,
    step_code: str,
    body: dict[str, Any],
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    x_caller_id: str | None = Header(default=None, alias="X-Caller-Id"),
) -> dict[str, Any]:
    _authenticate(x_api_key, x_caller_id)
    task = _task(task_id)
    if step_code not in STEP_DEPENDENCIES:
        raise HTTPException(status_code=404, detail="step is not registered")
    requested = body.get("status")
    if requested not in {"Success", "Failed"}:
        raise HTTPException(status_code=422, detail="step terminal status is required")
    step = task["steps"][step_code]
    if step["status"] != "Running":
        raise HTTPException(status_code=409, detail="step was not started")
    step["status"] = requested
    step["output"] = body.get("output")
    successful = sum(
        item["status"] == "Success" for item in task["steps"].values()
    )
    task["progress"] = int(successful * 100 / len(task["steps"]))
    return {"taskId": task_id, "stepCode": step_code, "status": requested}


@app.get("/api/v1/execution/tasks/{task_id}")
def get_task(
    task_id: str,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    x_caller_id: str | None = Header(default=None, alias="X-Caller-Id"),
) -> dict[str, Any]:
    _authenticate(x_api_key, x_caller_id)
    return _task(task_id)
