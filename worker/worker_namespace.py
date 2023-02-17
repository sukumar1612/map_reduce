import json

import socketio

from services.models import deserialize_file_model, deserialize_task
from services.worker.worker_api_interface import WorkerAPIInterface


class WorkerNamespace(socketio.ClientNamespace):
    def on_connect(self):
        print("______connected to server______")

    def on_assign_node_id(self, message_body: dict):
        WorkerAPIInterface.WORKER_ID = int(message_body["node_id"])
        print(f"connected to master, worker node id : {WorkerAPIInterface.WORKER_ID}")

    def on_worker_node_initialization(self, message_body: dict):
        print(message_body.get("completed"))
        if message_body.get("completed", None) is True:
            return
        file_chunk = deserialize_file_model(json.loads(message_body["chunk"]))
        print(f"__received chunk {file_chunk.chunk_index}__")
        WorkerAPIInterface.build_csv_file_from_chunks(file_chunk.chunk)

    def on_add_task(self, message_body: dict):
        WorkerAPIInterface.add_new_task(
            task=deserialize_task(json.loads(message_body["task"]))
        )
        self.emit(
            "get_map_results",
            {"map_keys": WorkerAPIInterface.perform_mapping_and_return_distinct_keys()},
            namespace="/worker",
        )
        print("___________performed mapping___________")

    def on_insert_reduce_keys(self, message_body: dict):
        WorkerAPIInterface.insert_reduce_keys_for_current_node(
            key_list=message_body["key_list"]
        )
        WorkerAPIInterface.shuffle(
            connected_nodes_ip=message_body["connected_nodes_ip"]
        )
        self.emit(
            "get_final_result",
            WorkerAPIInterface.perform_reduce_and_get_final_results(),
            namespace="/worker",
        )

    def on_prepare_for_next_task(self, message_body: dict):
        WorkerAPIInterface.prepare_for_next_task()

    def on_reset_state(self, message_body: dict):
        print(f"reset worker node : {WorkerAPIInterface.WORKER_ID}")
        WorkerAPIInterface.reset_state()
