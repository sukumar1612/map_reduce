import json

import socketio

from common.logger_module import get_logger
from common.models import deserialize_file_model, deserialize_task
from worker.services.worker_api_interface import TaskTracker

LOG = get_logger(__name__)


class WorkerNamespace(socketio.ClientNamespace):
    def on_connect(self):
        LOG.debug("connected to the server")

    def on_assign_node_id(self, message_body: dict):
        TaskTracker.WORKER_ID = int(message_body["node_id"])
        LOG.debug(f"connected to master, worker node id : {TaskTracker.WORKER_ID}")

    def on_worker_node_initialization(self, message_body: dict):
        if message_body.get("completed", None) is True:
            LOG.debug("---received final chunk---")
            self.emit(
                "file_init_done",
                {},
                namespace="/worker",
            )
            return
        file_chunk = deserialize_file_model(json.loads(message_body["chunk"]))
        LOG.debug(f"received chunk: {file_chunk.chunk_index}")
        TaskTracker.build_csv_file_from_chunks(file_chunk.chunk)

    def on_add_task(self, message_body: dict):
        TaskTracker.add_new_task(
            task=deserialize_task(json.loads(message_body["task"]))
        )
        self.emit(
            "get_map_results",
            {"map_keys": TaskTracker.perform_mapping_and_return_distinct_keys()},
            namespace="/worker",
        )
        LOG.debug("--mapping successful--")

    def on_insert_reduce_keys(self, message_body: dict):
        TaskTracker.insert_reduce_keys_for_current_node(
            key_list=message_body["key_list"]
        )
        TaskTracker.shuffle(connected_nodes_ip=message_body["connected_nodes_ip"])
        self.emit(
            "get_final_result",
            TaskTracker.perform_reduce_and_get_final_results(),
            namespace="/worker",
        )

    def on_prepare_for_next_task(self, message_body: dict):
        TaskTracker.prepare_for_next_task()

    def on_reset_state(self, message_body: dict):
        LOG.debug(f"reset worker node : {TaskTracker.WORKER_ID}")
        TaskTracker.reset_state()
