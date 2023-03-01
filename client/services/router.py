import enum
from typing import Callable, Dict, Union

import socketio


class EventTypes(enum.Enum):
    HTTP = "http"
    WEBSOCKET = "websocket"
    COMMAND = "command"


class BluePrint:
    def __init__(self):
        self.routes: Dict[str, dict] = {}

    def add_route(self, event: str, event_type: EventTypes) -> Callable:
        def decorator(route_handler: Callable):
            self.routes[event] = {
                "handler": route_handler,
                "event_type": event_type,
                "description": route_handler.__doc__,
            }
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
        for key, value in blue_print.routes.items():
            self.routes[key] = value

    def get_route_handler(self, event: str) -> Union[dict, None]:
        return self.routes.get(event, None)

    async def client_send(self):
        while True:
            event = input("{}: ".format("input command"))
            if event == "help":
                for key, value in self.routes.items():
                    print(f"\n-> command :: {key}")
                    print(f"description:: {value['description']}")

            route_meta_data = self.get_route_handler(event)
            if route_meta_data is None:
                print("______NO SUCH EVENT______")
            elif route_meta_data.get("event_type") == EventTypes.WEBSOCKET:
                await self.sio.connect(
                    self.sio_base_url,
                    socketio_path=self.sio_path,
                    namespaces=["/client"],
                )
                await route_meta_data.get("handler")(self.sio)
            elif route_meta_data.get("event_type") == EventTypes.HTTP:
                route_meta_data.get("handler")(self.http_base_url)
            elif route_meta_data.get("event_type") == EventTypes.COMMAND:
                route_meta_data.get("handler")()

    async def run(self):
        await self.client_send()
