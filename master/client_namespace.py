import asyncio
import json
import os
from pathlib import Path

import socketio

from master.redis_client_wrapper import RedisHandler
from services.master.master_api_interface import MasterAPIInterface
from services.models import deserialize_task


class ClientConnectionNamespace(socketio.AsyncNamespace):
    async def on_connect(self, sid: str, environ: dict):
        print("__client connected__")

    async def on_file_initialization(self, sid, message_body: dict):
        MasterAPIInterface.RECORD_FILE = open(
            os.path.join(
                Path(os.path.dirname(__file__)).parent, "master/random_data_1.csv"
            )
        )
        await MasterAPIInterface.initialize_all_worker_nodes_with_file_data(
            socket_connection=self
        )

    async def on_add_and_distribute_task(self, sid: str, message_body: dict):
        await MasterAPIInterface.add_and_distribute_task(
            task=deserialize_task(json.loads(message_body["task"])),
            socket_connection=self,
        )

    async def on_trigger_task_queue(self, sid: str, message_body: dict):
        asyncio.create_task(MasterAPIInterface.trigger_task_queue(self))

    async def on_reset_state(self, sid: str, message_body: dict):
        print("__master reset__")
        MasterAPIInterface.reset_state()
        for (
            node_id,
            node_meta_data,
        ) in MasterAPIInterface.CONNECTED_NODES_METADATA.items():
            await self.emit(
                "reset_state", {}, room=node_meta_data["sid"], namespace="/worker"
            )

    async def on_get_results(self, sid: str, message_body: dict):
        print(message_body["job_id"])
        await self.emit(
            "result",
            MasterAPIInterface.RESULTS.get(message_body["job_id"]),
            room=sid,
            namespace="/client",
        )
