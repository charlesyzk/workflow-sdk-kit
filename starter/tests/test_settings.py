"""配置兼容与启动期校验测试。

这些测试不读取仓库 `.env`，确保结果不依赖开发机上的真实凭据。
"""

import pytest
from pydantic import ValidationError

from obei_workflow_sdk import WorkflowSettings


def base_config(**overrides):
    """返回纯代码模式的最小合法配置，并允许单测覆盖目标字段。"""

    values = {
        "WORKFLOW_DATABASE_URL": "sqlite:///settings.db",
        "REDIS_URL": "redis://localhost:6379/0",
        "EXECUTION_TASK_ENABLED": False,
    }
    values.update(overrides)
    return values


def test_event_bus_falls_back_to_main_redis():
    settings = WorkflowSettings(_env_file=None, **base_config())
    assert settings.resolved_event_bus_url == "redis://localhost:6379/0"

    dedicated = WorkflowSettings(
        _env_file=None,
        **base_config(EVENT_BUS_URL="redis://events:6379/1"),
    )
    assert dedicated.resolved_event_bus_url == "redis://events:6379/1"


def test_dify_chat_key_is_preferred_but_legacy_key_remains_compatible():
    legacy = WorkflowSettings(_env_file=None, **base_config(DIFY_API_KEY="legacy-key"))
    assert legacy.resolved_dify_api_key == "legacy-key"

    preferred = WorkflowSettings(
        _env_file=None,
        **base_config(DIFY_API_KEY="legacy-key", DIFY_CHAT_API_KEY="chat-key"),
    )
    assert preferred.resolved_dify_api_key == "chat-key"

    blank_new_key = WorkflowSettings(
        _env_file=None,
        **base_config(DIFY_API_KEY="legacy-key", DIFY_CHAT_API_KEY=""),
    )
    assert blank_new_key.resolved_dify_api_key == "legacy-key"


def test_enabled_task_system_requires_real_connection_settings():
    with pytest.raises(ValidationError, match="enabled execution-task integration"):
        WorkflowSettings(
            _env_file=None,
            **base_config(EXECUTION_TASK_ENABLED=True),
        )


def test_retry_and_lock_invariants_are_validated_at_startup():
    with pytest.raises(ValidationError, match="retry base seconds"):
        WorkflowSettings(
            _env_file=None,
            **base_config(
                EXECUTION_TASK_RETRY_BASE_SECONDS=10,
                EXECUTION_TASK_RETRY_MAX_SECONDS=5,
            ),
        )

    with pytest.raises(ValidationError, match="renew seconds"):
        WorkflowSettings(
            _env_file=None,
            **base_config(
                WORKFLOW_RUN_LOCK_TTL_SECONDS=30,
                WORKFLOW_RUN_LOCK_RENEW_SECONDS=30,
            ),
        )
