"""Run the generated four-stage template against real infrastructure.

This maintainer-only script deliberately keeps the newly issued execution-task Key in process
memory. It does not update ``.env``, the generated template, logs, or result JSON. The script
starts a local API and both ARQ workers, submits through HTTP, captures SSE, verifies TiDB rows,
and queries the bound remote task before terminating every child process it created.

Prerequisites:

* the template source has been installed once (the build validation does this);
* a reachable Redis is supplied through ``INTEGRATION_REDIS_URL`` or the default localhost;
* the repository root ``.env`` contains TiDB and model configuration;
* real mode additionally needs the task-system Base URL, Caller ID, and a newly issued Key.

Use ``--mock-task-system`` when the remote service is unavailable. That mode still runs the real
ExecutionTaskAdapter, Redis queues, ARQ sync Worker, SQL Outbox, and Binding; only the HTTP peer is
replaced by ``scripts.mock_execution_task_server``.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
from getpass import getpass
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import urlparse
from typing import Any

import httpx
from dotenv import dotenv_values
from sqlalchemy import func, select
from sqlalchemy.orm import Session


KIT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = KIT_ROOT / ".env"
API_BASE_URL = os.environ.get("INTEGRATION_API_BASE_URL", "http://127.0.0.1:18091")
REDIS_URL = os.environ.get("INTEGRATION_REDIS_URL", "redis://127.0.0.1:6379/0")
MOCK_TASK_BASE_URL = "http://127.0.0.1:18092"
MOCK_TASK_API_KEY = "mock-execution-task-key"
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED", "BLOCKED"}


def _child_flags() -> int:
    """Keep child tools in the current background session without opening Windows consoles."""

    if os.name != "nt":
        return 0
    return subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW


def _wait_http(client: httpx.Client, processes: list[subprocess.Popen[Any]]) -> None:
    """Wait for API readiness while also detecting an immediately failed child process."""

    deadline = time.monotonic() + 40
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(
                    f"child process exited during startup: pid={process.pid}, code={process.returncode}"
                )
        try:
            health = client.get(f"{API_BASE_URL}/health")
            ready = client.get(f"{API_BASE_URL}/ready")
            health.raise_for_status()
            ready.raise_for_status()
            return
        except (httpx.HTTPError, OSError) as exc:
            last_error = exc
            time.sleep(0.5)
    raise RuntimeError(f"API did not become ready: {last_error}")


def _first_nested(value: Any, names: set[str]) -> Any:
    """Find one selected remote status field without persisting the whole remote response."""

    if isinstance(value, dict):
        for key, item in value.items():
            if key in names:
                return item
        for item in value.values():
            found = _first_nested(item, names)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _first_nested(item, names)
            if found is not None:
                return found
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the four-stage integration workflow")
    parser.add_argument(
        "--mock-task-system",
        action="store_true",
        help="replace only the unavailable remote task-system HTTP service with a strict local mock",
    )
    arguments = parser.parse_args()

    if not ENV_PATH.is_file():
        raise SystemExit(f"missing repository environment file: {ENV_PATH}")

    # dotenv_values parses URL-encoded passwords without shell expansion. Only non-empty values
    # enter the child environment, then the new Key and integration Redis override the snapshot.
    configured = {
        key: str(value)
        for key, value in dotenv_values(ENV_PATH).items()
        if value is not None and str(value) != ""
    }
    # Maintainers may target another deployment (for example production instead of UAT) without
    # editing the repository .env.  An explicit process-level override is conventional for
    # twelve-factor configuration and, importantly, keeps deployment URLs out of distributable
    # artifacts.  Only this non-secret endpoint value is overridden here; API Keys still enter via
    # the hidden prompt below and never become command-line arguments.
    if os.environ.get("EXECUTION_TASK_API_BASE_URL"):
        configured["EXECUTION_TASK_API_BASE_URL"] = os.environ[
            "EXECUTION_TASK_API_BASE_URL"
        ]
    required = {
        "WORKFLOW_DATABASE_URL",
        "OPENAI_BASE_URL",
        "OPENAI_API_KEY",
        "OPENAI_MODEL",
    }
    if not arguments.mock_task_system:
        required.update(
            {"EXECUTION_TASK_API_BASE_URL", "EXECUTION_TASK_CALLER_ID"}
        )
    missing = sorted(required - configured.keys())
    if missing:
        raise SystemExit(f"missing integration settings: {', '.join(missing)}")

    if arguments.mock_task_system:
        # A fixed non-secret test credential is shared only with the local mock child process.
        execution_key = MOCK_TASK_API_KEY
        configured["EXECUTION_TASK_API_BASE_URL"] = MOCK_TASK_BASE_URL
        configured["EXECUTION_TASK_CALLER_ID"] = "workbench-service"
    else:
        execution_key = getpass("New execution-task API Key (input is hidden): ").strip()
        if not execution_key:
            raise SystemExit("execution-task API Key cannot be empty")

        # The developer machine may use an HTTP(S) proxy for public model traffic.  Internal task
        # system TLS should go directly to its origin: routing it through that proxy can stall the
        # TLS handshake before the server has a chance to authenticate the API Key.  Append only
        # the task-system host (plus loopback); do not disable proxying globally because the model
        # endpoint may still depend on the developer's proxy/VPN.
        task_system_host = urlparse(
            configured["EXECUTION_TASK_API_BASE_URL"]
        ).hostname
        if not task_system_host:
            raise SystemExit("EXECUTION_TASK_API_BASE_URL must contain a valid host")
        no_proxy_hosts = ["127.0.0.1", "localhost", task_system_host]
        existing_no_proxy = os.environ.get("NO_PROXY") or os.environ.get("no_proxy", "")
        existing_hosts = [item.strip() for item in existing_no_proxy.split(",") if item.strip()]
        merged_no_proxy = ",".join(dict.fromkeys([*existing_hosts, *no_proxy_hosts]))
        configured["NO_PROXY"] = merged_no_proxy
        configured["no_proxy"] = merged_no_proxy

    child_environment = os.environ.copy()
    child_environment.update(configured)
    child_environment.update(
        {
            "EXECUTION_TASK_ENABLED": "true",
            "EXECUTION_TASK_API_KEY": execution_key,
            "REDIS_URL": REDIS_URL,
            "EVENT_BUS_URL": REDIS_URL,
            "MOCK_EXECUTION_TASK_API_KEY": MOCK_TASK_API_KEY,
        }
    )
    # The current process also needs the same snapshot for settings, ORM inspection, and the
    # authenticated remote GET. The Key is never included in printed or serialized structures.
    os.environ.update(child_environment)

    from obei_workflow_sdk.models import (
        ExecutionBinding,
        LLMInvocation,
        TaskSystemOutbox,
        WorkflowArtifact,
        WorkflowCheckpoint,
        WorkflowEvent,
        WorkflowNodeExecution,
        WorkflowRun,
        WorkflowTask,
    )
    from obei_workflow_sdk.settings import WorkflowSettings
    from obei_workflow_sdk.storage import SQLAlchemyWorkflowStorage

    settings = WorkflowSettings()  # type: ignore[call-arg]
    storage = SQLAlchemyWorkflowStorage(settings.database_url, create_tables=False)
    arq_executable = shutil.which("arq")
    curl_executable = shutil.which("curl") or shutil.which("curl.exe")
    if not arq_executable or not curl_executable:
        raise SystemExit("arq and curl executables are required")

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    result_directory = KIT_ROOT / "integration-results" / stamp
    result_directory.mkdir(parents=True, exist_ok=False)
    mode_name = "mock" if arguments.mock_task_system else "real"
    actor_id = f"template-{mode_name}-integration-{stamp}"
    processes: list[subprocess.Popen[Any]] = []
    log_handles: list[Any] = []
    curl_process: subprocess.Popen[Any] | None = None
    task_id: str | None = None
    result: dict[str, Any] = {
        "started_at": datetime.now().isoformat(),
        "actor_id": actor_id,
        "api_base_url": API_BASE_URL,
        "redis_url": REDIS_URL,
        "task_system_mode": mode_name,
    }

    def start_process(name: str, command: list[str]) -> subprocess.Popen[Any]:
        stdout_handle = (result_directory / f"{name}.stdout.log").open(
            "w", encoding="utf-8"
        )
        stderr_handle = (result_directory / f"{name}.stderr.log").open(
            "w", encoding="utf-8"
        )
        log_handles.extend([stdout_handle, stderr_handle])
        process = subprocess.Popen(
            command,
            cwd=KIT_ROOT,
            env=child_environment,
            stdout=stdout_handle,
            stderr=stderr_handle,
            creationflags=_child_flags(),
        )
        processes.append(process)
        return process

    try:
        if arguments.mock_task_system:
            mock_process = start_process(
                "mock-task-system",
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "scripts.mock_execution_task_server:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    MOCK_TASK_BASE_URL.rsplit(":", 1)[-1],
                ],
            )
            mock_deadline = time.monotonic() + 30
            with httpx.Client(timeout=5) as mock_client:
                while time.monotonic() < mock_deadline:
                    if mock_process.poll() is not None:
                        raise RuntimeError(
                            f"mock task system exited with code {mock_process.returncode}"
                        )
                    try:
                        mock_health = mock_client.get(f"{MOCK_TASK_BASE_URL}/health")
                        mock_health.raise_for_status()
                        break
                    except httpx.HTTPError:
                        time.sleep(0.3)
                else:
                    raise RuntimeError("mock task system did not become ready")

        # Explicit migration is part of the real startup contract. It is run before API/Worker so
        # no process can accept a task against partially initialized tables.
        migration = subprocess.run(
            [sys.executable, "-m", "workflow_app.migrate"],
            cwd=KIT_ROOT,
            env=child_environment,
            capture_output=True,
            text=True,
            timeout=60,
            creationflags=_child_flags(),
            check=False,
        )
        if migration.returncode != 0:
            raise RuntimeError(f"migration failed: {migration.stderr[-1000:]}")

        api_process = start_process(
            "api",
            [
                sys.executable,
                "-m",
                "uvicorn",
                "workflow_app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                API_BASE_URL.rsplit(":", 1)[-1],
            ],
        )
        sync_process = start_process(
            "sync-worker",
            [arq_executable, "workflow_app.worker.ExecutionSyncWorkerSettings"],
        )

        with httpx.Client(timeout=15) as client:
            _wait_http(client, [api_process, sync_process])
            accepted_response = client.post(
                f"{API_BASE_URL}/api/v1/tasks",
                json={
                    "workflow_type": "simple_llm_workflow",
                    "actor_id": actor_id,
                    "input": {
                        "question": "请用三句话说明持久化工作流状态对企业任务系统的价值。"
                    },
                },
            )
            accepted_response.raise_for_status()
            accepted = accepted_response.json()
            task_id = str(accepted["task_id"])
            result.update(
                {
                    "task_id": task_id,
                    "run_id": str(accepted["run_id"]),
                    "submit_http_status": accepted_response.status_code,
                }
            )

            # The workflow Worker starts only after curl has subscribed. The ARQ message is already
            # durable in Redis, so this ordering captures even the first custom context_token.
            sse_handle = (result_directory / "events.sse").open("wb")
            sse_error_handle = (result_directory / "events.stderr.log").open("wb")
            log_handles.extend([sse_handle, sse_error_handle])
            curl_process = subprocess.Popen(
                [
                    curl_executable,
                    "-N",
                    "--silent",
                    "--show-error",
                    "--max-time",
                    "600",
                    f"{API_BASE_URL}/api/v1/tasks/{task_id}/events",
                ],
                cwd=KIT_ROOT,
                env=child_environment,
                stdout=sse_handle,
                stderr=sse_error_handle,
                creationflags=_child_flags(),
            )
            time.sleep(0.8)
            workflow_process = start_process(
                "workflow-worker",
                [arq_executable, "workflow_app.worker.WorkflowWorkerSettings"],
            )

            deadline = time.monotonic() + 600
            task: dict[str, Any] | None = None
            while time.monotonic() < deadline:
                if workflow_process.poll() is not None:
                    raise RuntimeError(
                        f"workflow Worker exited early with code {workflow_process.returncode}"
                    )
                response = client.get(f"{API_BASE_URL}/api/v1/tasks/{task_id}")
                response.raise_for_status()
                task = response.json()
                if task.get("status") in TERMINAL_STATUSES:
                    break
                time.sleep(1)
            if not task or task.get("status") not in TERMINAL_STATUSES:
                raise TimeoutError("workflow did not reach a terminal state within 600 seconds")
            result["task"] = {
                "status": task.get("status"),
                "current_node": task.get("current_node"),
                "has_final_artifact": bool(task.get("final_artifact_id")),
            }

            trace_response = client.get(f"{API_BASE_URL}/api/v1/tasks/{task_id}/trace")
            trace_response.raise_for_status()
            llm_response = client.get(
                f"{API_BASE_URL}/api/v1/tasks/{task_id}/llm-invocations"
            )
            llm_response.raise_for_status()
            trace_payload = trace_response.json()
            result["http_trace_node_statuses"] = [
                [item.get("node_name"), item.get("status")]
                for item in trace_payload.get("nodes", [])
            ]
            result["http_llm_stream_statuses"] = [
                [item.get("node_name"), item.get("stream"), item.get("status")]
                for item in llm_response.json().get("items", [])
            ]

        # Wait until the independent sync Worker has consumed every final Outbox fact. A failed row
        # is terminal too, but is preserved and causes validation failure below.
        outbox_deadline = time.monotonic() + 180
        while time.monotonic() < outbox_deadline:
            with storage.session_factory() as database:
                pending = database.scalar(
                    select(func.count(TaskSystemOutbox.id)).where(
                        TaskSystemOutbox.local_task_id == task_id,
                        TaskSystemOutbox.status == "PENDING",
                    )
                )
            if not pending:
                break
            time.sleep(1)

        with storage.session_factory() as database:
            task_row = database.get(WorkflowTask, task_id)
            run_row = database.scalar(
                select(WorkflowRun)
                .where(WorkflowRun.task_id == task_id)
                .order_by(WorkflowRun.created_at.desc())
            )
            binding = database.scalar(
                select(ExecutionBinding)
                .where(ExecutionBinding.local_task_id == task_id)
                .order_by(ExecutionBinding.retry_seq.desc())
            )
            nodes = database.scalars(
                select(WorkflowNodeExecution)
                .where(WorkflowNodeExecution.run_id == run_row.id)
                .order_by(WorkflowNodeExecution.started_at)
            ).all()
            invocations = database.scalars(
                select(LLMInvocation)
                .where(LLMInvocation.task_id == task_id)
                .order_by(LLMInvocation.started_at)
            ).all()
            artifacts = database.scalars(
                select(WorkflowArtifact)
                .where(WorkflowArtifact.task_id == task_id)
                .order_by(WorkflowArtifact.created_at)
            ).all()
            outbox = database.scalars(
                select(TaskSystemOutbox)
                .where(TaskSystemOutbox.local_task_id == task_id)
                .order_by(TaskSystemOutbox.event_seq)
            ).all()
            checkpoint_count = database.scalar(
                select(func.count(WorkflowCheckpoint.id)).where(
                    WorkflowCheckpoint.run_id == run_row.id
                )
            )
            events = database.scalars(
                select(WorkflowEvent).where(WorkflowEvent.task_id == task_id)
            ).all()

            result["database"] = {
                "task_status": task_row.status,
                "run_status": run_row.status,
                "node_statuses": [[item.node_name, item.status] for item in nodes],
                "llm_invocations": [
                    [
                        item.node_name,
                        bool(item.stream),
                        item.status,
                        item.duration_ms,
                        len(item.response_text or ""),
                        len(item.reasoning_text or ""),
                    ]
                    for item in invocations
                ],
                "artifacts": [
                    [item.artifact_type, item.version, item.size_bytes]
                    for item in artifacts
                ],
                "checkpoint_count": int(checkpoint_count or 0),
                "persistent_event_count": len(events),
                "binding": {
                    "status": binding.binding_status if binding else None,
                    "external_status": binding.external_status if binding else None,
                    "has_external_task_id": bool(binding and binding.external_task_id),
                    "external_task_id": binding.external_task_id if binding else None,
                    "external_task_code": binding.external_task_code if binding else None,
                },
                "outbox_status_counts": dict(Counter(item.status for item in outbox)),
                "outbox_events": [
                    [item.event_seq, item.event_type, item.step_code, item.status]
                    for item in outbox
                ],
            }

        if curl_process is not None and curl_process.poll() is None:
            curl_process.terminate()
            curl_process.wait(timeout=10)
        for handle in log_handles:
            handle.flush()
        sse_text = (result_directory / "events.sse").read_text(
            encoding="utf-8", errors="replace"
        )
        event_names = [
            line.removeprefix("event: ").strip()
            for line in sse_text.splitlines()
            if line.startswith("event: ")
        ]
        result["sse_event_counts"] = dict(Counter(event_names))

        if binding and binding.external_task_id:
            remote_url = (
                settings.execution_task_api_base_url.rstrip("/")
                + f"/api/v1/execution/tasks/{binding.external_task_id}"
            )
            with httpx.Client(
                headers={
                    "X-API-Key": execution_key,
                    "X-Caller-Id": settings.execution_task_caller_id,
                },
                timeout=settings.execution_task_timeout_seconds,
                # This client talks only to the internal task-system host.  Explicitly ignoring
                # proxy environment variables makes the diagnostic query deterministic even when
                # a locally running proxy changes or overrides NO_PROXY matching rules.
                trust_env=False,
            ) as remote_client:
                remote_response = remote_client.get(remote_url)
            try:
                remote_body: Any = remote_response.json()
            except ValueError:
                remote_body = None
            raw_progress = _first_nested(remote_body, {"progress", "percentage"})
            # Deployments expose progress in two compatible shapes: mocks/older gateways return
            # the percentage directly, while production returns a structured progress object with
            # totalSteps, completedSteps and percentage.  Normalize both before validation so a
            # genuine 4/4 production completion is not reported as a false failure.
            normalized_progress = (
                _first_nested(raw_progress, {"percentage"})
                if isinstance(raw_progress, dict)
                else raw_progress
            )
            result["remote_query"] = {
                "http_status": remote_response.status_code,
                "top_level_keys": sorted(remote_body.keys())
                if isinstance(remote_body, dict)
                else [],
                "status": _first_nested(remote_body, {"status", "taskStatus"}),
                "business_status": _first_nested(
                    remote_body, {"businessStatus", "business_status"}
                ),
                "progress": normalized_progress,
            }

        database_result = result["database"]
        errors: list[str] = []
        if result["task"]["status"] != "SUCCEEDED":
            errors.append(f"local task status is {result['task']['status']}")
        if [item[1] for item in database_result["node_statuses"]] != ["SUCCEEDED"] * 4:
            errors.append("not all four nodes succeeded")
        if [item[1] for item in database_result["llm_invocations"]] != [True, False]:
            errors.append("LLM stream flags are not [true, false]")
        if [item[2] for item in database_result["llm_invocations"]] != [
            "SUCCEEDED",
            "SUCCEEDED",
        ]:
            errors.append("one or more LLM invocations failed")
        if [item[0] for item in database_result["artifacts"]] != ["draft", "answer"]:
            errors.append("draft and answer Artifacts were not both persisted")
        if database_result["binding"]["status"] != "BOUND":
            errors.append("remote task binding is not BOUND")
        if database_result["outbox_status_counts"].get("FAILED", 0):
            errors.append("one or more task-system Outbox rows failed")
        if database_result["outbox_status_counts"].get("PENDING", 0):
            errors.append("one or more task-system Outbox rows remained pending")
        if database_result["outbox_status_counts"].get("SUCCEEDED", 0) != 11:
            errors.append("task-system Outbox did not deliver all 11 ordered events")
        if result["sse_event_counts"].get("context_token", 0) < 2:
            errors.append("SSE did not contain both custom context_token events")
        if result["sse_event_counts"].get("llm_token", 0) < 1:
            errors.append("SSE did not contain streaming LLM tokens")
        remote_query = result.get("remote_query") or {}
        if remote_query.get("http_status") != 200:
            errors.append("bound task could not be queried from the task system")
        if remote_query.get("status") != "Success":
            errors.append("task-system task did not reach Success")
        if remote_query.get("progress") != 100:
            errors.append("task-system task progress did not reach 100")

        result["validation_errors"] = errors
        result["succeeded"] = not errors
        result["finished_at"] = datetime.now().isoformat()
        result_path = result_directory / "result.json"
        result_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"Integration result: {result_path}")
        return 0 if not errors else 1
    except Exception as exc:
        result.update(
            {
                "succeeded": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "finished_at": datetime.now().isoformat(),
            }
        )
        result_path = result_directory / "result.json"
        result_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        print(f"Integration result: {result_path}")
        return 1
    finally:
        if curl_process is not None and curl_process.poll() is None:
            curl_process.terminate()
            try:
                curl_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                curl_process.kill()
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
        for handle in log_handles:
            try:
                handle.close()
            except OSError:
                pass
        # Remove the Key from this process environment as soon as all authenticated work ends.
        os.environ.pop("EXECUTION_TASK_API_KEY", None)


if __name__ == "__main__":
    raise SystemExit(main())
