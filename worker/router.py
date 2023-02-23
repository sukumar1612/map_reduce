import threading

import eventlet
import socketio

from worker.p2p_server import app
from worker.worker_namespace import WorkerNamespace

sio_worker = socketio.Client()
sio_worker.register_namespace(WorkerNamespace("/worker"))


def worker_client_thread(host: str):
    print(f"http://{host}:5000/ws/socket.io/")
    sio_worker.connect(
        f"http://{host}:5000/", socketio_path="ws/socket.io", namespaces=["/worker"]
    )
    sio_worker.wait()


def worker_server_thread(host: str):
    print(host)
    eventlet.wsgi.server(eventlet.listen((host, 7000)), app)


def worker_process(p2p_server_host: str, master_server_host: str):
    t1 = threading.Thread(target=worker_client_thread, args=(master_server_host,))
    t2 = threading.Thread(target=worker_server_thread, args=(p2p_server_host,))
    t1.daemon = True
    t2.daemon = True
    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == "__main__":
    worker_process(host="localhost")
