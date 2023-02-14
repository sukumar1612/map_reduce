import asyncio
import json

import dill
import socketio
import websockets

from services.models import (FileModel, WebSocketMessage, WorkerTask,
                             serialize_file_model, serialize_task)
from unit_tests.simulation.map_and_reduce_functions import (MapFunction,
                                                            ReduceFunction)

CHUNK_SIZE = 1024 * 100
TEST_FILE = "random_data"


async def test_add_new_task(websocket):
    task = WorkerTask(
        map_function=dill.dumps(MapFunction),
        reduce_function=dill.dumps(ReduceFunction),
        node_id=1,
    )
    add_task = WebSocketMessage(event="add_new_task", body=serialize_task(task))
    await websocket.send(add_task.json())
    response = await websocket.recv()
    if response == "ACK":
        print("Success")


def file_chunking():
    file = open(f"../unit_tests/simulation/{TEST_FILE}.csv", "rb")
    chunks = []
    temp_chunk = file.read(CHUNK_SIZE)
    while temp_chunk:
        chunks.append(temp_chunk)
        temp_chunk = file.read(CHUNK_SIZE)

    for index, chunk in enumerate(chunks):
        file_chunk = WebSocketMessage(
            event="file_stream",
            body=serialize_file_model(
                FileModel(chunk=chunk, chunk_index=index, completed=False)
            ),
        ).json()
        print(f"sent chunk: {index + 1}")
        print(file_chunk)
        yield file_chunk


async def test_file_streaming_new(websocket):
    for file_chunk in file_chunking():
        await websocket.send(file_chunk)
        response = await websocket.recv()
        if response == "ACK":
            print("Success")

    await websocket.send(
        WebSocketMessage(
            event="file_stream",
            body=serialize_file_model(
                FileModel(chunk="", chunk_index=-1, completed=True)
            ),
        ).json()
    )
    response = await websocket.recv()
    if response == "ACK":
        print("Done")


async def test_perform_mapping_and_get_distinct_keys(websocket):
    await websocket.send(
        WebSocketMessage(
            event="distinct_keys",
            body={},
        ).json()
    )
    response = await websocket.recv()
    print(response)


async def assign_key_for_reduce(websocket):
    await websocket.send(
        WebSocketMessage(
            event="assign_reduce_keys_and_shuffle",
            body={
                "key_list": ["apple"],
                "other_worker_node_ip": ["ws://localhost:8765", "ws://localhost:8765"],
            },
        ).json()
    )
    print(json.loads(await websocket.recv())["apple"] // 4)


async def all_tests():
    async with websockets.connect("ws://localhost:8765", timeout=40) as websocket:
        await test_file_streaming_new(websocket)
        await test_add_new_task(websocket)
        await test_perform_mapping_and_get_distinct_keys(websocket)
        await assign_key_for_reduce(websocket)


async def test_initialization_sequence(websocket):
    await websocket.send(
        WebSocketMessage(
            event="initialization_sequence",
            body={},
        ).json()
    )


async def master_tests():
    async with websockets.connect("ws://localhost:5000", timeout=40) as websocket:
        await test_initialization_sequence(websocket)


sio = socketio.Client()

if __name__ == "__main__":
    sio.connect('http://localhost:5000', namespaces=['/client'])
    # sio.emit('file_initialization', {}, namespace='/client')
    # sio.emit('perform_map_reduce', {}, namespace='/client')
    sio.emit('environment', {}, namespace='/client')
    sio.wait()
