-- Run as a TiDB account with INDEX privilege.
-- The application account can create tables but cannot create these secondary indexes.

CREATE INDEX ix_sdk_task_outbox_task_seq
    ON obei_workshop_task_system_outbox (local_task_id, event_seq);

CREATE INDEX ix_sdk_llm_invocation_run_node
    ON obei_workshop_llm_invocation (run_id, node_name);

CREATE INDEX ix_sdk_dify_conversation_task
    ON obei_workshop_dify_conversation (task_id);

CREATE INDEX ix_sdk_dify_invocation_run_node
    ON obei_workshop_dify_invocation (run_id, node_name);
