import asyncio
import time

import redis.asyncio as redis_async
from redis import Redis


class RedisHandler:
    def __init__(self, host: str, pub_sub_channel: str):
        self.host = host
        self.redis_host = redis_async.Redis(host=host, port=6379)
        self.pub_sub_channel = pub_sub_channel

    def publish_to_channel(self, message: str) -> None:
        Redis(host=self.host, port=6379).publish(self.pub_sub_channel, message)

    async def subscribe_to_channel(self):
        sub = self.redis_host.pubsub()
        await sub.subscribe(self.pub_sub_channel)
        while True:
            msg = await sub.get_message()
            if msg is not None:
                if type(msg.get("data")) == bytes:
                    yield msg.get("data").decode("ascii")
            await asyncio.sleep(2)


if __name__ == "__main__":
    h = RedisHandler(host="localhost", pub_sub_channel="task_queue")
    h.publish_to_channel('{"message": "hello world"}')
