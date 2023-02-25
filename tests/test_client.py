import json
import marshal
import pprint
import time

import socketio

from master.redis_client_wrapper import RedisHandler
from services.models import Task, serialize_task
from tests.test_map_reduce_functions import (MapFunction, MapFunction1,
                                             ReduceFunction, ReduceFunction1)

sio = socketio.Client()


@sio.on("result", namespace="/client")
def on_result(message_body: dict):
    pprint.pprint(message_body)


if __name__ == "__main__":
    task1 = Task(
        mapper_function=marshal.dumps(MapFunction.__code__),
        reducer_function=marshal.dumps(ReduceFunction.__code__),
    )
    job_id = task1.job_id

    task2 = Task(
        mapper_function=marshal.dumps(MapFunction1.__code__),
        reducer_function=marshal.dumps(ReduceFunction1.__code__),
    )
    job_id2 = task2.job_id
    print(job_id, job_id2)
    sio.connect(
        "ws://localhost:5000", socketio_path="ws/socket.io", namespaces=["/client"]
    )
    sio.emit("file_initialization", {}, namespace="/client")
    print("__file initialized__")
    time.sleep(7)
    sio.emit(
        "trigger_task_queue",
        {},
        namespace="/client",
    )
    print("__task queue reader started__")
    time.sleep(3)
    serialized_task = json.dumps(serialize_task(task1))
    h = RedisHandler(host="localhost", pub_sub_channel="task_queue")
    h.publish_to_channel(serialized_task)

    print("__added another task")
    serialized_task = json.dumps(serialize_task(task2))
    h = RedisHandler(host="localhost", pub_sub_channel="task_queue")
    h.publish_to_channel(serialized_task)

    print("__tasks submitted__")
    time.sleep(10)
    print("__reset_state__")
    # sio.emit("reset_state", {}, namespace="/client")
    # time.sleep(3)
    sio.emit("get_results", {"job_id": job_id}, namespace="/client")
    time.sleep(3)
    sio.emit("get_results", {"job_id": job_id2}, namespace="/client")
    time.sleep(3)
    print("_done_")
    sio.wait()
