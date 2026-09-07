"""FastAPI 应用入口。"""

from fastapi import FastAPI

from obei_workflow_sdk import create_workflow_router

from .container import get_runtime


runtime = get_runtime()
app = FastAPI(
    title="Obei Workflow Project",
    version="0.1.0",
    description="基于内置 Obei Workflow SDK 的工作流服务模板",
)

# SDK 提供提交、查询、Trace、SSE、Artifact、LLM 审计、人工决定、重试、取消和
# 注册定义接口。正式接入时，应由业务项目在这一层增加认证、租户和权限中间件。
app.include_router(create_workflow_router(runtime))


@app.get("/health")
def health() -> dict:
    """只验证应用进程已启动以及工作流注册成功，不访问外部基础设施。"""

    return {"status": "ok", "workflow_types": runtime.registry.types()}


@app.get("/ready")
def ready() -> dict:
    """验证数据库连接；部署平台可把该接口用作 readiness probe。"""

    with runtime.storage.engine.connect() as connection:
        connection.exec_driver_sql("SELECT 1")
    return {"status": "ready"}
