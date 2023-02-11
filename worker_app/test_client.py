import asyncio

import dill
import websockets
from services.models import (
    FileModel,
    WebSocketMessage,
    WorkerTask,
    serialize_file_model,
    serialize_task,
)
from unit_tests.simulation.map_and_reduce_functions import MapFunction, ReduceFunction

CHUNK_SIZE = 1024


async def test_add_new_task(websocket):
    task = WorkerTask(
        file_name="random_data.csv",
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
    file = open("../unit_tests/simulation/random_data.csv", "rb")
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


async def all_tests():
    async with websockets.connect("ws://localhost:8765", timeout=40) as websocket:
        await test_add_new_task(websocket)
        await test_file_streaming_new(websocket)


if __name__ == "__main__":
    asyncio.run(all_tests())
