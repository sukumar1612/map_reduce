import json
import time

import dill
import socketio

from services.models import Task, serialize_task
from tests.test_map_reduce_functions import MapFunction, ReduceFunction

sio = socketio.Client()

if __name__ == "__main__":
    sio.connect("ws://localhost:5000", namespaces=["/client"])
    sio.emit("file_initialization", {}, namespace="/client")
    print("__file initialized__")
    time.sleep(10)
    sio.emit(
        "add_and_distribute_task",
        {
            "task": json.dumps(
                serialize_task(
                    Task(
                        mapper_function=dill.dumps(MapFunction),
                        reducer_function=dill.dumps(ReduceFunction),
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
    # sio.emit('assign_reduce_keys_and_perform_reduce', {}, namespace='/client')
    sio.wait()
