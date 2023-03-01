import json

from rich.progress import Progress, SpinnerColumn, TextColumn, track
from socketio import AsyncClient, Client

from client.services.router import BluePrint, EventTypes
from common.models import FileModel, serialize_file_model

websocket_handler = BluePrint()
CHUNK_SIZE = 1024 * 100


async def stream_file(sio: AsyncClient):
    file = open("random_data_5.csv", "rb")
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
    print(f"sent total of {index} chunks")


@websocket_handler.add_route(event="init_sequence", event_type=EventTypes.WEBSOCKET)
async def init_sequence(sio: AsyncClient):
    """stream file to master node and start the job queue"""
    await stream_file(sio)
    await sio.emit(
        "trigger_task_queue",
        {},
        namespace="/client",
    )
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(
            description="waiting for all worker nodes to initialized...", total=None
        )
        await sio.wait()


@websocket_handler.add_route(event="file_stream", event_type=EventTypes.WEBSOCKET)
async def file_stream(sio: AsyncClient):
    """stream file to master node"""
    await stream_file(sio)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        progress.add_task(
            description="waiting for all worker nodes to initialized...", total=None
        )
        await sio.wait()


@websocket_handler.add_route(event="reset_state", event_type=EventTypes.WEBSOCKET)
async def reset_state(sio: AsyncClient):
    """reset state of all nodes including master and workers"""
    await sio.emit("reset_state", {}, namespace="/client")
    await sio.wait()
