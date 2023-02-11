import asyncio
import json

import websockets
from services.models import WebSocketMessage

from worker_app.router import Router


async def handler(websocket):
    async for message in websocket:
        deserialized_message = WebSocketMessage.parse_obj(json.loads(message))
        route_handler = Router.get_route_handler(event=deserialized_message.event)
        await route_handler(websocket, deserialized_message.body)


async def main():
    async with websockets.serve(handler, "localhost", 8765):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
