import json

from rich.progress import Progress, SpinnerColumn, TextColumn, track
from socketio import AsyncClient, Client

from client.services.router import BluePrint
from common.models import FileModel, serialize_file_model

websocket_handler = BluePrint()
CHUNK_SIZE = 1024 * 100


@websocket_handler.add_socketio_route(event="file_init")
async def file_init(sio: AsyncClient):
    file = open("random_data_3.csv", "rb")
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
    print(f"sent total of {index} chunks")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(
            description="waiting for all worker nodes to initialized...", total=None
        )
        await sio.wait()


@websocket_handler.add_socketio_route(event="reset_state")
async def reset_state(sio: AsyncClient):
    await sio.emit("reset_state", {}, namespace="/client")
    await sio.wait()
