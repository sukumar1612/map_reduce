import json

import socketio

from services.master.master_api_interface import MasterAPIInterface
from services.models import deserialize_task


class ClientConnectionNamespace(socketio.Namespace):
    def on_connect(self, sid: str, environ: dict):
        print("__client connected__")

    def on_file_initialization(self, sid, message_body: dict):
        MasterAPIInterface.RECORD_FILE = open("random_data_1.csv")
        MasterAPIInterface.initialize_all_worker_nodes_with_file_data(
            socket_connection=self
        )

    def on_add_and_distribute_task(self, sid: str, message_body: dict):
        MasterAPIInterface.add_and_distribute_task(
            task=deserialize_task(json.loads(message_body["task"])),
            socket_connection=self,
        )

    def on_reset_state(self, sid: str, message_body: dict):
        print("__master reset__")
        MasterAPIInterface.reset_state()
        for (
            node_id,
            node_meta_data,
        ) in MasterAPIInterface.CONNECTED_NODES_METADATA.items():
            self.emit(
                "reset_state", {}, room=node_meta_data["sid"], namespace="/worker"
            )
