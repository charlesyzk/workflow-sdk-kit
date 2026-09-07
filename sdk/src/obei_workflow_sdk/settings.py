"""SDK 运行配置。

这里只描述通用工作流运行时所需的基础设施，不引入 Text2SQL、Neo4j、
Chroma 等具体业务配置。API 与 ARQ Worker 必须使用同一份配置，避免两类
进程连接到不同的数据库、Redis 或外部任务系统。
"""

from pydantic import Field, model_validator
from typing import Literal
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkflowSettings(BaseSettings):
    """从环境变量或当前目录的 ``.env`` 构建生产运行配置。

    任务系统默认启用，保持 Starter 的生产行为；纯代码宿主可以显式设置
    ``EXECUTION_TASK_ENABLED=false``。关闭仅表示不建立外部任务绑定，本地七张
    核心表、节点生命周期、事件和模型审计仍然正常工作。
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # 工作流事实、Checkpoint、Outbox 与 LLM 审计共用这个事务数据库。
    database_url: str = Field(alias="WORKFLOW_DATABASE_URL")
    # ARQ 队列、运行锁的默认 Redis；事件总线可通过 EVENT_BUS_URL 单独部署。
    redis_url: str = Field(alias="REDIS_URL")
    event_bus_url: str = Field(default="", alias="EVENT_BUS_URL")
    public_base_url: str = Field(default="", alias="WORKFLOW_PUBLIC_BASE_URL")

    # 租约必须在过期前续期；校验器会保证 renew < ttl。
    run_lock_ttl_seconds: int = Field(default=90, gt=0, alias="WORKFLOW_RUN_LOCK_TTL_SECONDS")
    run_lock_renew_seconds: int = Field(default=30, gt=0, alias="WORKFLOW_RUN_LOCK_RENEW_SECONDS")
    arq_job_timeout_seconds: int = Field(default=600, gt=0, alias="ARQ_JOB_TIMEOUT_SECONDS")
    arq_workflow_queue: str = Field(default="workflow", min_length=1, alias="ARQ_WORKFLOW_QUEUE")
    arq_execution_sync_queue: str = Field(default="execution_sync", min_length=1, alias="ARQ_EXECUTION_SYNC_QUEUE")
    arq_workflow_max_jobs: int = Field(default=4, gt=0, alias="ARQ_WORKFLOW_MAX_JOBS")
    arq_execution_sync_max_jobs: int = Field(default=2, gt=0, alias="ARQ_EXECUTION_SYNC_MAX_JOBS")

    # Redis Stream 只承载实时事件副本和流式 token，数据库事件表仍是可恢复事实源。
    event_stream_prefix: str = Field(default="workflow", min_length=1, alias="EVENT_STREAM_PREFIX")
    event_stream_maxlen: int = Field(default=20_000, gt=0, alias="EVENT_STREAM_MAXLEN")
    event_stream_block_ms: int = Field(default=10_000, gt=0, alias="EVENT_STREAM_BLOCK_MS")

    # 外部任务系统采用 SQL Outbox 可靠投递。禁用时下面三个连接字段允许为空。
    execution_task_enabled: bool = Field(default=True, alias="EXECUTION_TASK_ENABLED")
    execution_task_api_base_url: str = Field(default="", alias="EXECUTION_TASK_API_BASE_URL")
    execution_task_api_key: str = Field(default="", alias="EXECUTION_TASK_API_KEY")
    execution_task_caller_id: str = Field(default="", alias="EXECUTION_TASK_CALLER_ID")
    execution_task_execution_mode: str = Field(default="RecordOnly", alias="EXECUTION_TASK_EXECUTION_MODE")
    execution_task_timeout_seconds: int = Field(default=10, gt=0, alias="EXECUTION_TASK_TIMEOUT_SECONDS")
    execution_task_retry_max_attempts: int = Field(default=8, gt=0, alias="EXECUTION_TASK_RETRY_MAX_ATTEMPTS")
    execution_task_retry_base_seconds: int = Field(default=1, gt=0, alias="EXECUTION_TASK_RETRY_BASE_SECONDS")
    execution_task_retry_max_seconds: int = Field(default=300, gt=0, alias="EXECUTION_TASK_RETRY_MAX_SECONDS")
    # 远端只接收步骤摘要；完整模型输入输出由 LLM 审计表保存。
    execution_task_output_max_bytes: int = Field(default=8192, gt=0, alias="EXECUTION_TASK_OUTPUT_MAX_BYTES")

    # OpenAI-compatible Adapter；API Key 为空表示不注册该 Adapter。
    openai_base_url: str = Field(default="", alias="OPENAI_BASE_URL")
    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    openai_model: str = Field(default="", alias="OPENAI_MODEL")
    openai_timeout_seconds: int = Field(default=120, gt=0, alias="OPENAI_TIMEOUT_SECONDS")

    # SDK 调用 /chat-messages，因此优先使用聊天应用 Key。旧字段单独保留，才能在
    # 新字段已声明但为空时仍正确回退，而不是把空字符串误认为有效配置。
    dify_base_url: str = Field(default="", alias="DIFY_BASE_URL")
    dify_chat_api_key: str = Field(default="", alias="DIFY_CHAT_API_KEY")
    dify_api_key: str = Field(default="", alias="DIFY_API_KEY", exclude=True)
    dify_timeout_seconds: int = Field(default=120, gt=0, alias="DIFY_TIMEOUT_SECONDS")
    dify_user: str = Field(default="obei-workflow-sdk", alias="DIFY_USER")
    # Multi-app mode. Each JSON item references its dedicated secret environment variable through
    # api_key_env, so credentials do not need to be embedded in this registry description.
    dify_apps_json: str = Field(default="", alias="DIFY_APPS_JSON")
    dify_default_app_name: str = Field(default="dify-chat-app", alias="DIFY_DEFAULT_APP_NAME")
    dify_default_app_type: Literal["chat", "agent-chat", "agent", "advanced-chat", "completion", "workflow"] = Field(default="chat", alias="DIFY_DEFAULT_APP_TYPE")
    dify_conversation_lock_ttl_seconds: int = Field(default=180, gt=0, alias="DIFY_CONVERSATION_LOCK_TTL_SECONDS")
    dify_conversation_lock_renew_seconds: int = Field(default=30, gt=0, alias="DIFY_CONVERSATION_LOCK_RENEW_SECONDS")
    dify_conversation_lock_wait_seconds: int = Field(default=180, gt=0, alias="DIFY_CONVERSATION_LOCK_WAIT_SECONDS")

    @property
    def resolved_event_bus_url(self) -> str:
        """返回事件流 Redis；未单独配置时复用主 Redis。"""

        return self.event_bus_url or self.redis_url

    @property
    def resolved_dify_api_key(self) -> str:
        """优先返回 Chat Key；为空时兼容旧版 ``DIFY_API_KEY``。"""

        return self.dify_chat_api_key or self.dify_api_key

    @model_validator(mode="after")
    def validate_runtime_invariants(self):
        """在进程启动阶段阻止不可恢复或语义不完整的配置进入运行时。"""

        if self.run_lock_renew_seconds >= self.run_lock_ttl_seconds:
            raise ValueError("run lock requires 0 < renew seconds < TTL seconds")
        if self.execution_task_retry_base_seconds > self.execution_task_retry_max_seconds:
            raise ValueError("execution-task retry base seconds cannot exceed retry max seconds")
        if self.execution_task_enabled:
            missing = not (
                self.execution_task_api_base_url
                and self.execution_task_api_key
                and self.execution_task_api_key != "change-me"
                and self.execution_task_caller_id
            )
            if missing:
                raise ValueError(
                    "enabled execution-task integration requires a real API base URL, API key and caller ID"
                )
        if self.dify_conversation_lock_renew_seconds >= self.dify_conversation_lock_ttl_seconds:
            raise ValueError("Dify conversation lock requires renew seconds < TTL seconds")
        return self
