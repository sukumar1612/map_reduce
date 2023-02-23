# import eventlet
# import socketio
#
# from master.client_namespace import ClientConnectionNamespace
# from master.worker_namespace import WorkerNamespace
#
# sio_master = socketio.Server()
# app = socketio.WSGIApp(sio_master)
#
# sio_master.register_namespace(ClientConnectionNamespace("/client"))
# sio_master.register_namespace(WorkerNamespace("/worker"))
import socketio
from fastapi import FastAPI

from master.client_namespace import ClientConnectionNamespace
from master.worker_namespace import WorkerNamespace

app = FastAPI()

_sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")

_sio.register_namespace(ClientConnectionNamespace("/client"))
_sio.register_namespace(WorkerNamespace("/worker"))

_app = socketio.ASGIApp(socketio_server=_sio, socketio_path="socket.io")

app.mount("/ws", _app)
