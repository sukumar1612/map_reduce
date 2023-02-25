import asyncio
from typing import AsyncGenerator


class TaskQueueSingleton:
    QUEUE: asyncio.Queue = asyncio.LifoQueue(maxsize=100)

    @classmethod
    async def enqueue(cls, message: str):
        await cls.QUEUE.put(message)

    @classmethod
    async def dequeue(cls) -> AsyncGenerator:
        while True:
            message = await cls.QUEUE.get()
            yield message
