import socketio
from fastapi import FastAPI

from master.namespaces.client import ClientConnectionNamespace
from master.namespaces.worker import WorkerNamespace
from master.rest_api.router import app

fastapi_app = FastAPI()

sio = socketio.AsyncServer(
    async_mode="asgi", cors_allowed_origins="*", ping_timeout=480
)

sio.register_namespace(ClientConnectionNamespace("/client"))
sio.register_namespace(WorkerNamespace("/worker"))

fastapi_app.mount(
    "/ws", socketio.ASGIApp(socketio_server=sio, socketio_path="socket.io")
)
fastapi_app.mount("/rest", app)
