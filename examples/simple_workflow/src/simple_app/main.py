"""FastAPI 入口。"""

from fastapi import FastAPI
from obei_workflow_sdk import create_workflow_router

from .container import get_runtime


runtime = get_runtime()
app = FastAPI(title="Simple Workflow Example", version="0.1.0")
app.include_router(create_workflow_router(runtime))


@app.get("/health")
def health():
    return {"status": "ok", "workflow_types": runtime.registry.types()}


@app.get("/ready")
def ready():
    with runtime.storage.engine.connect() as connection:
        connection.exec_driver_sql("SELECT 1")
    return {"status": "ready"}

