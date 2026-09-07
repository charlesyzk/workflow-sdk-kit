import asyncio
import uuid
from contextlib import asynccontextmanager

import redis.asyncio as redis
import time


class WorkflowRunLocked(RuntimeError):
    pass


class WorkflowRunLockLost(RuntimeError):
    pass


class RedisWorkflowRunLock:
    """带所有权令牌和自动续租的 Redis Run 级分布式锁。"""
    _RENEW = "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('expire',KEYS[1],ARGV[2]) end return 0"
    _RELEASE = "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0"

    def __init__(self, redis_url: str, run_id: str, ttl_seconds: int, renew_seconds: int):
        self.client = redis.from_url(redis_url, decode_responses=True)
        self.key = f"workflow:run-lock:{run_id}"
        self.owner = uuid.uuid4().hex
        self.ttl = ttl_seconds
        self.renew = renew_seconds
        self.stop = asyncio.Event()
        self.lost = asyncio.Event()
        self.task = None

    async def _renew_loop(self):
        """仅锁仍属于当前 owner 时续租；异常或所有权丢失都会置 lost 标志。"""
        try:
            while not self.stop.is_set():
                try:
                    await asyncio.wait_for(self.stop.wait(), self.renew)
                except TimeoutError:
                    if int(await self.client.eval(self._RENEW, 1, self.key, self.owner, self.ttl) or 0) != 1:
                        self.lost.set()
                        return
        except Exception:
            self.lost.set()

    def ensure_owned(self):
        """在提交最终状态前确认本 Worker 仍拥有执行权。"""
        if self.lost.is_set():
            raise WorkflowRunLockLost(self.key)

    @asynccontextmanager
    async def hold(self):
        """获取、后台续租并以 compare-and-delete 语义安全释放锁。"""
        if not await self.client.set(self.key, self.owner, nx=True, ex=self.ttl):
            await self.client.aclose()
            raise WorkflowRunLocked(self.key)
        self.task = asyncio.create_task(self._renew_loop())
        try:
            yield self
            self.ensure_owned()
        finally:
            self.stop.set()
            if self.task:
                await self.task
            try:
                await self.client.eval(self._RELEASE, 1, self.key, self.owner)
            finally:
                await self.client.aclose()


class RedisDifyConversationLock(RedisWorkflowRunLock):
    """Waitable distributed lock serializing turns in one Dify conversation."""

    def __init__(self, redis_url: str, identity: str, ttl_seconds: int, renew_seconds: int, wait_seconds: int):
        super().__init__(redis_url, identity, ttl_seconds, renew_seconds)
        self.key = f"workflow:dify-conversation-lock:{identity}"
        self.wait_seconds = wait_seconds

    @asynccontextmanager
    async def hold(self):
        """Wait for the previous turn, then keep the lock renewed for the whole SSE call."""

        deadline = time.monotonic() + self.wait_seconds
        while not await self.client.set(self.key, self.owner, nx=True, ex=self.ttl):
            if time.monotonic() >= deadline:
                await self.client.aclose()
                raise WorkflowRunLocked(f"timed out waiting for Dify conversation: {self.key}")
            await asyncio.sleep(0.1)
        self.task = asyncio.create_task(self._renew_loop())
        try:
            yield self
            self.ensure_owned()
        finally:
            self.stop.set()
            if self.task:
                await self.task
            try:
                await self.client.eval(self._RELEASE, 1, self.key, self.owner)
            finally:
                await self.client.aclose()
