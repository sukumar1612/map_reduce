from typing import Callable, Dict, Union

from services.models import WorkerTask, deserialize_file_model, deserialize_task

from worker_app.worker_network_interface import WorkerInterface


class Router:
    routes: Dict[str, Callable] = {}

    @classmethod
    def add_route(cls, event: str) -> Callable:
        def decorator(route_handler):
            cls.routes[event] = route_handler
            return route_handler

        return decorator

    @classmethod
    def get_route_handler(cls, event: str) -> Union[Callable, None]:
        route_handler = cls.routes.get(event, None)
        if route_handler is None:
            raise Exception("No Such Route")
        return route_handler


@Router.add_route(event="add_new_task")
async def add_new_task_handler(websocket, message_body: dict):
    deserialized_message_body = deserialize_task(message_body, WorkerTask)
    WorkerInterface.add_new_task(deserialized_message_body)
    await websocket.send("ACK")


@Router.add_route(event="file_stream")
async def build_csv_file_from_chunks_handler(websocket, message_body: dict):
    deserialized_message_body = deserialize_file_model(message_body)
    if deserialized_message_body.completed is True:
        WorkerInterface.CSV_FILE.seek(0)
        print(WorkerInterface.CSV_FILE.read())
        await websocket.send("ACK")
        return
    WorkerInterface.build_csv_file_from_chunks(chunk=deserialized_message_body.chunk)
    await websocket.send("ACK")
