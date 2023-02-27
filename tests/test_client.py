import json
import marshal
import pprint
import time

import requests
import socketio

from common.models import FileModel, Task, serialize_file_model, serialize_task
from tests.test_map_reduce_functions import (MapFunction, MapFunction1,
                                             ReduceFunction, ReduceFunction1)

sio = socketio.Client()
CHUNK_SIZE = 1024 * 100


@sio.on("result", namespace="/client")
def on_result(message_body: dict):
    pprint.pprint(message_body)


@sio.on("all_file_init_done", namespace="/client")
def on_all_file_init_done(message_body: dict):
    pprint.pprint(message_body)
    print("file init done")


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

    file = open("random_data_2.csv", "rb")
    chunks = []
    temp_chunk = file.read(CHUNK_SIZE)
    while temp_chunk:
        chunks.append(temp_chunk)
        temp_chunk = file.read(CHUNK_SIZE)

    for index, chunk in enumerate(chunks):
        file_chunk = json.dumps(
            serialize_file_model(
                FileModel(chunk=chunk, chunk_index=index, completed=False)
            )
        )
        sio.emit("file_initialization", {"chunk": file_chunk}, namespace="/client")
        print(f"sent chunk: {index + 1}")

    sio.emit("file_initialization", {"completed": True}, namespace="/client")
    sio.emit(
        "trigger_task_queue",
        {},
        namespace="/client",
    )
    print("__task queue reader started__")
    print("__file initialized done + task queue started__")
    time.sleep(20)
    serialized_task = json.dumps(serialize_task(task1))
    requests.post(
        url="http://localhost:5000/rest/add-task", json={"task": serialized_task}
    )

    print("__added another task__")
    serialized_task = json.dumps(serialize_task(task2))
    requests.post(
        url="http://localhost:5000/rest/add-task", json={"task": serialized_task}
    )

    print("__tasks submitted__")
    time.sleep(15)
    print("__reset_state__")
    # sio.emit("reset_state", {}, namespace="/client")
    # time.sleep(3)
    # sio.emit("get_results", {"job_id": job_id}, namespace="/client")
    # time.sleep(3)
    # sio.emit("get_results", {"job_id": job_id2}, namespace="/client")
    # time.sleep(3)

    pprint.pprint(
        requests.get(
            url="http://localhost:5000/rest/fetch-result", params={"job_id": job_id}
        ).json()
    )
    pprint.pprint(
        requests.get(
            url="http://localhost:5000/rest/fetch-result", params={"job_id": job_id2}
        ).json()
    )
    print("_done_")
    sio.wait()
