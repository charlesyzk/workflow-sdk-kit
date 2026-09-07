import json
from typing import Any

import redis
import redis.asyncio as async_redis


class RedisEventBus:
    """Redis Streams 实时事件总线；持久事实仍以数据库事件表为准。"""
    def __init__(self, redis_url: str, prefix: str = "workflow", maxlen: int = 20_000):
        self.redis_url, self.prefix, self.maxlen = redis_url, prefix, maxlen
        # redis-py clients are thread-safe and pool connections; reuse one client so
        # streaming tokens do not create a TCP connection per chunk.
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)

    def key(self, task_id: str) -> str:
        """按任务隔离 Stream，便于客户端独立游标读取。"""
        return f"{self.prefix}:events:{task_id}"

    def publish(self, task_id: str, payload: dict[str, Any]) -> str:
        """追加事件并近似裁剪旧 token，返回 Redis Stream message id。"""
        return str(self.client.xadd(self.key(task_id), {"payload": json.dumps(payload, ensure_ascii=False, default=str)}, maxlen=self.maxlen, approximate=True))

    def close(self) -> None:
        self.client.close()

    async def read(self, task_id: str, cursor: str, block_ms: int):
        """从游标阻塞读取下一批实时事件，每次调用后释放异步连接。"""
        client = async_redis.from_url(self.redis_url, decode_responses=True)
        try:
            rows = await client.xread({self.key(task_id): cursor}, count=100, block=block_ms)
            return [(message_id, json.loads(fields["payload"])) for _, messages in rows for message_id, fields in messages]
        finally:
            await client.aclose()

    async def tail_id(self, task_id: str) -> str:
        """取得当前尾游标，用于从订阅时刻开始只接收新事件。"""
        client = async_redis.from_url(self.redis_url, decode_responses=True)
        try:
            rows = await client.xrevrange(self.key(task_id), count=1)
            return rows[0][0] if rows else "0-0"
        finally:
            await client.aclose()
