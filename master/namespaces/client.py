import asyncio
import json

import socketio

from common.logger_module import get_logger
from common.models import deserialize_file_model
from master.services.master_api_interface import JobTracker

LOCK = asyncio.Lock()
LOG = get_logger(__name__)


class ClientConnectionNamespace(socketio.AsyncNamespace):
    async def on_connect(self, sid: str, environ: dict):
        LOG.debug(
            f"client connected with ip : http://{environ.get('asgi.scope').get('client')[0]}"
        )

    async def on_file_initialization(self, sid, message_body: dict):
        if message_body.get("completed", None) is True:
            await JobTracker.initialize_all_worker_nodes_with_file_data(
                socket_connection=self
            )
            return
        file_chunk = deserialize_file_model(json.loads(message_body["chunk"]))
        LOG.debug(f"received chunk {file_chunk.chunk_index}")
        async with LOCK:
            JobTracker.build_csv_file_from_chunks(file_chunk.chunk)

    async def on_trigger_task_queue(self, sid: str, message_body: dict):
        asyncio.create_task(JobTracker.trigger_task_queue(self))
        LOG.debug("Task queue started")

    async def on_reset_state(self, sid: str, message_body: dict):
        await JobTracker.reset_state(self, sid=sid)
        LOG.debug("master has been reset")
