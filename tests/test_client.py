import json
import marshal
import pprint
import time

import dill
import socketio

from services.models import Task, serialize_task
from tests.test_map_reduce_functions import MapFunction, ReduceFunction

sio = socketio.Client()


@sio.on("result", namespace="/client")
def on_result(message_body: dict):
    pprint.pprint(message_body)


if __name__ == "__main__":
    task = Task(
        mapper_function=marshal.dumps(MapFunction.__code__),
        reducer_function=marshal.dumps(ReduceFunction.__code__),
    )
    job_id = task.job_id
    sio.connect(
        "ws://localhost:5000", socketio_path="ws/socket.io", namespaces=["/client"]
    )
    sio.emit("file_initialization", {}, namespace="/client")
    print("__file initialized__")
    time.sleep(10)
    sio.emit(
        "add_and_distribute_task",
        {"task": json.dumps(serialize_task(task))},
        namespace="/client",
    )
    print("__task submitted__")
    time.sleep(10)
    print("__reset_state__")
    sio.emit("reset_state", {}, namespace="/client")
    time.sleep(3)
    sio.emit("get_results", {"job_id": job_id}, namespace="/client")
    sio.wait()
