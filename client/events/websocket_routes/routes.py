import json

from rich.progress import Progress, SpinnerColumn, TextColumn, track
from socketio import AsyncClient, Client

from client.services.router import BluePrint, EventTypes
from common.models import FileModel, serialize_file_model

websocket_handler = BluePrint()
CHUNK_SIZE = 1024 * 100


async def stream_file(sio: AsyncClient, *args):
    file = open(args[0], "rb")
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
async def init_sequence(sio: AsyncClient, *args):
    """stream file to master node and start the job queue"""
    await stream_file(sio, *args)
    await sio.emit(
        "trigger_task_queue",
        {},
        namespace="/client",
    )
    print("task queue has been triggered")
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
async def file_stream(sio: AsyncClient, *args):
    """stream file to master node"""
    await stream_file(sio, *args)
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
async def reset_state(sio: AsyncClient, *args):
    """reset state of all nodes including master and workers"""
    await sio.emit("reset_state", {}, namespace="/client")
    await sio.wait()
