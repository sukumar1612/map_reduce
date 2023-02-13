import asyncio
import json
from typing import Callable, Dict, Union

import websockets

from services.models import WebSocketMessage


class Router:
    def __init__(self):
        self.routes: Dict[str, Callable] = {}

    def add_route(self, event: str) -> Callable:
        def decorator(route_handler):
            self.routes[event] = route_handler
            return route_handler

        return decorator

    def get_route_handler(self, event: str) -> Union[Callable, None]:
        route_handler = self.routes.get(event, None)
        if route_handler is None:
            raise Exception("No Such Route")
        return route_handler

    def add_routes_from_other_routers(self, other_router):
        self.routes.update(other_router.routes)

    async def message_parser(self, websocket):
        async for message in websocket:
            deserialized_message = WebSocketMessage.parse_obj(json.loads(message))
            route_handler = self.get_route_handler(event=deserialized_message.event)
            await route_handler(websocket, deserialized_message.body)

    async def handler(self, host: str, port: int):
        async with websockets.serve(self.message_parser, host, port):
            await asyncio.Future()

    def run_app(self, host: str, port: int):
        asyncio.run(self.handler(host, port))
