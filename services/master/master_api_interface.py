import json
import tempfile
from io import TextIOWrapper
from typing import Union

import socketio

from services.master.master_node import MasterNode, master_node_factory
from services.models import Task, serialize_file_model, serialize_task


class MasterAPIInterface:
    RECORD_FILE: Union[TextIOWrapper, None] = None
    CURRENT_TASK: Union[Task, None] = None

    MASTER_NODE_HANDLER: Union[MasterNode, None] = None
    CONNECTED_NODES: int = 0
    CONNECTED_NODES_METADATA: dict = {}
    NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK: int = 0

    @classmethod
    def reset_state(cls):
        cls.RECORD_FILE.close()
        cls.CURRENT_TASK = None
        cls.MASTER_NODE_HANDLER = None
        cls.CONNECTED_NODES = 0
        cls.CONNECTED_NODES_METADATA = {}
        cls.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK = 0

    @classmethod
    def prepare_for_next_task(cls):
        cls.MASTER_NODE_HANDLER.reset_state()
        cls.CURRENT_TASK = None

    @classmethod
    def build_csv_file_from_chunks(cls, chunk: bytes):
        if cls.RECORD_FILE is None:
            cls.RECORD_FILE = tempfile.NamedTemporaryFile()
        cls.RECORD_FILE.write(chunk)

    @classmethod
    def add_new_node(cls, node_sid: str, node_ip: str) -> int:
        cls.CONNECTED_NODES_METADATA[cls.CONNECTED_NODES] = {
            "sid": node_sid,
            "ip": node_ip,
        }
        cls.CONNECTED_NODES += 1
        return cls.CONNECTED_NODES - 1

    @classmethod
    def initialize_all_worker_nodes_with_file_data(
        cls, socket_connection: socketio.Namespace
    ):
        cls.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK = cls.CONNECTED_NODES
        if cls.MASTER_NODE_HANDLER is None:
            cls.MASTER_NODE_HANDLER = master_node_factory(cls.RECORD_FILE.name)
        temp_file_list = (
            cls.MASTER_NODE_HANDLER.create_dataframe_chunks_as_temporary_file(
                worker_node_count=cls.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK
            )
        )
        for node_id, node_meta_data in cls.CONNECTED_NODES_METADATA.items():
            for file_chunk in cls.MASTER_NODE_HANDLER.file_chunking(
                temp_file_list[node_id]
            ):
                socket_connection.emit(
                    "worker_node_initialization",
                    {"chunk": file_chunk},
                    room=node_meta_data["sid"],
                    namespace="/worker",
                )
            socket_connection.emit(
                "worker_node_initialization",
                {"completed": True},
                room=node_meta_data["sid"],
                namespace="/worker",
            )
            print(f"sent file to node {node_id} successfully")

    @classmethod
    def add_and_distribute_task(cls, task: Task, socket_connection: socketio.Namespace):
        cls.CURRENT_TASK = task
        # broadcast task to all nodes under /worker
        task = json.dumps(serialize_task(cls.CURRENT_TASK))
        for node_id, node_meta_data in cls.CONNECTED_NODES_METADATA.items():
            socket_connection.emit(
                "add_task",
                {"task": task},
                room=node_meta_data["sid"],
                namespace="/worker",
            )
            print(f"____task sent to : {node_id}____")

    @classmethod
    def insert_map_result_data(cls, map_keys: list):
        cls.MASTER_NODE_HANDLER.insert_mapped_keys(map_keys)

    @classmethod
    def assign_reduce_keys(cls, socket_connection: socketio.Namespace) -> None:
        node_reduce_map = (
            cls.MASTER_NODE_HANDLER.assign_reduce_key_to_workers_round_robin(
                cls.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK
            )
        )
        for node_id, node_meta_data in cls.CONNECTED_NODES_METADATA.items():
            socket_connection.emit(
                "insert_reduce_keys",
                {
                    "key_list": node_reduce_map[node_id],
                    "connected_nodes_ip": {
                        _id: ip["ip"]
                        for _id, ip in cls.CONNECTED_NODES_METADATA.items()
                        if _id != node_id
                    },
                },
                room=node_meta_data["sid"],
                namespace="/worker",
            )

    @classmethod
    def insert_partial_result(cls, result: dict):
        cls.MASTER_NODE_HANDLER.aggregate_reduced_data(result)
