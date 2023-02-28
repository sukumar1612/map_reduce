import asyncio
from typing import Callable, Dict, Union

import socketio


class BluePrint:
    def __init__(self):
        self.http_routes: Dict[str, Callable] = {}
        self.socketio_routes: Dict[str, Callable] = {}
        self.terminal_commands: Dict[str, Callable] = {}

    def add_http_route(self, event: str) -> Callable:
        def decorator(route_handler):
            self.http_routes[event] = route_handler
            return route_handler

        return decorator

    def add_socketio_route(self, event: str) -> Callable:
        def decorator(route_handler):
            self.socketio_routes[event] = route_handler
            return route_handler

        return decorator

    def add_terminal_commands(self, event: str) -> Callable:
        def decorator(route_handler):
            self.terminal_commands[event] = route_handler
            return route_handler

        return decorator


class Router(BluePrint):
    def __init__(self, ip: str, port: int):
        super().__init__()
        self.sio = socketio.AsyncClient()
        self.sio_base_url = f"ws://{ip}:{port}"
        self.sio_path = "ws/socket.io"
        self.http_base_url = f"http://{ip}:{port}/rest"

    def add_from_blue_print(self, blue_print: BluePrint):
        for key, value in blue_print.http_routes.items():
            self.http_routes[key] = value
        for key, value in blue_print.socketio_routes.items():
            self.socketio_routes[key] = value
        for key, value in blue_print.terminal_commands.items():
            self.terminal_commands[key] = value

    def get_http_route_handler(self, event: str) -> Union[Callable, None]:
        return self.http_routes.get(event, None)

    def get_socketio_route_handler(self, event: str) -> Union[Callable, None]:
        return self.socketio_routes.get(event, None)

    def get_terminal_command_handler(self, event: str) -> Union[Callable, None]:
        return self.terminal_commands.get(event, None)

    async def client_send(self):
        while True:
            event = input("{}: ".format("input command"))
            http_route = self.get_http_route_handler(event)
            socketio_route = self.get_socketio_route_handler(event)
            command = self.get_terminal_command_handler(event)

            if http_route is None and socketio_route is None and command is None:
                raise Exception("Not a valid event")
            elif socketio_route is not None:
                await self.sio.connect(
                    self.sio_base_url,
                    socketio_path=self.sio_path,
                    namespaces=["/client"],
                )
                await socketio_route(self.sio, self.sio_base_url, self.sio_path)
            elif http_route is not None:
                http_route(self.http_base_url)
            elif command is not None:
                command()

    async def run(self):
        await self.client_send()
