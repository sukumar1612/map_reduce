import asyncio
import marshal

from client.events.http_routes.routes import http_handler
from client.events.terminal_commands.commands import commands_handler
from client.events.test_map_reduce_functions import (MapFunction, MapFunction1,
                                                     ReduceFunction,
                                                     ReduceFunction1)
from client.events.websocket_routes.namespaces import \
    ClientNamespaceResponseHandlers
from client.events.websocket_routes.routes import websocket_handler
from client.services.router import Router
from client.services.state_manager import StateManager
from common.models import Task

app = Router(ip="localhost", port=5000)
app.add_from_blue_print(http_handler)
app.add_from_blue_print(websocket_handler)
app.add_from_blue_print(commands_handler)
app.sio.register_namespace(ClientNamespaceResponseHandlers("/client"))

if __name__ == "__main__":
    task1 = Task(
        mapper_function=marshal.dumps(MapFunction.__code__),
        reducer_function=marshal.dumps(ReduceFunction.__code__),
    )
    task2 = Task(
        mapper_function=marshal.dumps(MapFunction1.__code__),
        reducer_function=marshal.dumps(ReduceFunction1.__code__),
    )
    StateManager.jobs = [task1, task2]
    asyncio.run(app.run())
