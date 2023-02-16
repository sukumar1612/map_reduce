import eventlet
import socketio

from master.client_namespace import ClientConnectionNamespace
from master.worker_namespace import WorkerNamespace

sio_master = socketio.Server()
app = socketio.WSGIApp(sio_master)

sio_master.register_namespace(ClientConnectionNamespace("/client"))
sio_master.register_namespace(WorkerNamespace("/worker"))

if __name__ == "__main__":
    eventlet.wsgi.server(eventlet.listen(("", 5000)), app)
