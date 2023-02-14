import asyncio
import json
from typing import Callable, Dict, Union

import websockets

from services.models import WebSocketMessage


class Router:
    def __init__(self, node_type="master"):
        self.routes: Dict[str, Callable] = {}
        self.node_type = node_type

    def add_route(self, event: str) -> Callable:
        def decorator(route_handler):
            self.routes[event] = route_handler
            return route_handler

        return decorator

    def get_route_handler(self, event: str) -> Union[Callable, None]:
        route_handler = self.routes.get(event, None)
        if route_handler is None:
            print("ACK")
            # raise Exception("No Such Route")
        return route_handler

    def add_routes_from_other_routers(self, other_router):
        self.routes.update(other_router.routes)


class ClientRouter(Router):
    @staticmethod
    async def initialize_connection(websocket, ClientInterface):
        await websocket.send(
            json.dumps(WebSocketMessage(
                event="add_worker_node",
                body={"node_id": ClientInterface.get_worker_node_id()},
            ).dict())
        )
        response = await websocket.recv()
        deserialized_message = WebSocketMessage.parse_obj(json.loads(response))
        ClientInterface.set_worker_node_id(int(deserialized_message.body['node_id']))
        print(f"node initialized to {int(deserialized_message.body['node_id'])}")

    async def handler(self, host: str, port: int, ClientInterface):
        initialized = 0
        connector = websockets.connect(f'ws://{host}:{port}', timeout=40)
        async for websocket in connector:
            try:
                if not initialized:
                    await self.initialize_connection(websocket, ClientInterface)
                    initialized = 1
                next_message = await websocket.recv()
                deserialized_message = WebSocketMessage.parse_obj(json.loads(next_message))
                route_handler = self.get_route_handler(event=deserialized_message.event)
                if route_handler is None:
                    continue
                await route_handler(websocket, deserialized_message.body)
            except websockets.ConnectionClosed:
                print("________connection closed__________")
                initialized = 0

    def connect_server(self, host: str, port: int, ClientInterface):
        asyncio.run(self.handler(host, port, ClientInterface))


class ServerRouter(Router):

    async def message_parser(self, websocket):
        async for message in websocket:
            print(message)
            deserialized_message = WebSocketMessage.parse_obj(json.loads(message))
            route_handler = self.get_route_handler(event=deserialized_message.event)
            await route_handler(websocket, deserialized_message.body)

    async def handler(self, host: str, port: int):
        async with websockets.serve(self.message_parser, host, port):
            await asyncio.Future()

    def run_app(self, host: str, port: int):
        asyncio.run(self.handler(host, port))
