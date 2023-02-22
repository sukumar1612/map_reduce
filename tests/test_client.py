import json
import marshal
import time

import dill
import socketio

from services.models import Task, serialize_task
from tests.test_map_reduce_functions import MapFunction, ReduceFunction

sio = socketio.Client()

if __name__ == "__main__":
    sio.connect("ws://10.1.82.79:5000", namespaces=["/client"])
    sio.emit("file_initialization", {}, namespace="/client")
    print("__file initialized__")
    time.sleep(10)
    sio.emit(
        "add_and_distribute_task",
        {
            "task": json.dumps(
                serialize_task(
                    Task(
                        mapper_function=marshal.dumps(MapFunction.__code__),
                        reducer_function=marshal.dumps(ReduceFunction.__code__),
                    )
                )
            )
        },
        namespace="/client",
    )
    print("__task submitted__")
    time.sleep(10)
    print("__reset_state__")
    sio.emit("reset_state", {}, namespace="/client")
    sio.wait()
