import asyncio
import json

import socketio

from common.models import deserialize_file_model
from master.services.master_api_interface import MasterAPIInterface

LOCK = asyncio.Lock()


class ClientConnectionNamespace(socketio.AsyncNamespace):
    async def on_connect(self, sid: str, environ: dict):
        print("__client connected__")

    async def on_file_initialization(self, sid, message_body: dict):
        if message_body.get("completed", None) is True:
            await MasterAPIInterface.initialize_all_worker_nodes_with_file_data(
                socket_connection=self
            )
            return
        file_chunk = deserialize_file_model(json.loads(message_body["chunk"]))
        print(f"__received chunk {file_chunk.chunk_index}__")
        async with LOCK:
            MasterAPIInterface.build_csv_file_from_chunks(file_chunk.chunk)

    async def on_trigger_task_queue(self, sid: str, message_body: dict):
        asyncio.create_task(MasterAPIInterface.trigger_task_queue(self))

    async def on_reset_state(self, sid: str, message_body: dict):
        print("__master reset__")
        await MasterAPIInterface.reset_state(self)
