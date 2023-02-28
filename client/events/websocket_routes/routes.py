import json

from socketio import AsyncClient, Client
from rich.progress import track

from client.services.router import BluePrint
from common.models import FileModel, serialize_file_model

websocket_handler = BluePrint()
CHUNK_SIZE = 1024 * 100


@websocket_handler.add_socketio_route(event="file_init")
async def file_init(sio: AsyncClient, sio_base_url: str, sio_path: str):
    file = open("random_data_2.csv", "rb")
    chunks = []
    temp_chunk = file.read(CHUNK_SIZE)
    while temp_chunk:
        chunks.append(temp_chunk)
        temp_chunk = file.read(CHUNK_SIZE)

    index = 0
    for chunk in track(chunks, description="Processing..."):
        file_chunk = json.dumps(
            serialize_file_model(
                FileModel(chunk=chunk, chunk_index=index, completed=False)
            )
        )
        await sio.emit(
            "file_initialization", {"chunk": file_chunk}, namespace="/client"
        )
        index += 1

    await sio.emit("file_initialization", {"completed": True}, namespace="/client")
    await sio.emit(
        "trigger_task_queue",
        {},
        namespace="/client",
    )
    print(f'sent total of {index} chunks')
    await sio.wait()


@websocket_handler.add_socketio_route(event="reset_state")
async def reset_state(sio: AsyncClient, sio_base_url: str, sio_path: str):
    await sio.emit("reset_state", {}, namespace="/client")
