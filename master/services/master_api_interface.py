import asyncio
import datetime
import json
import tempfile
from io import TextIOWrapper
from typing import Union

import socketio

from common.models import Task, deserialize_task, serialize_task
from master.services.master_node_handler import MasterNode, master_node_factory
from master.services.task_queue import TaskQueueSingleton


class MasterAPIInterface:
    TASK_COMPLETE = asyncio.Event()
    RECORD_FILE: Union[TextIOWrapper, None] = None
    CURRENT_TASK: Union[Task, None] = None

    MASTER_NODE_HANDLER: Union[MasterNode, None] = None
    CONNECTED_NODES: int = 0
    CONNECTED_NODES_METADATA: dict = {}
    NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK: int = 0

    RESULTS: dict = {}

    @classmethod
    async def reset_state(cls, socket_connection: socketio.Namespace, sid: str):
        cls.RECORD_FILE.close()
        cls.CURRENT_TASK = None
        cls.MASTER_NODE_HANDLER = None
        cls.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK = 0
        cls.TASK_COMPLETE.set()

        for node_id, node_meta_data in cls.CONNECTED_NODES_METADATA.items():
            await socket_connection.emit(
                "reset_state", {}, room=node_meta_data["sid"], namespace="/worker"
            )
        await socket_connection.emit("reset_done", {}, room=sid, namespace="/client")

    @classmethod
    def prepare_for_next_task(cls):
        cls.RESULTS[cls.CURRENT_TASK.job_id].update(
            {
                "result": cls.MASTER_NODE_HANDLER.get_final_reduced_data(),
                "end_time": datetime.datetime.now().timestamp(),
            }
        )
        cls.MASTER_NODE_HANDLER.reset_state()
        cls.CURRENT_TASK = None
        cls.TASK_COMPLETE.set()

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
    async def initialize_all_worker_nodes_with_file_data(
        cls, socket_connection: socketio.Namespace
    ):
        cls.RECORD_FILE.seek(0)
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
                await socket_connection.emit(
                    "worker_node_initialization",
                    {"chunk": file_chunk},
                    room=node_meta_data["sid"],
                    namespace="/worker",
                )
            await socket_connection.emit(
                "worker_node_initialization",
                {"completed": True},
                room=node_meta_data["sid"],
                namespace="/worker",
            )
            print(f"sent file to node {node_id} successfully")

    @classmethod
    async def add_and_distribute_task(
        cls, task: Task, socket_connection: socketio.Namespace
    ):
        cls.CURRENT_TASK = task
        # broadcast task to all nodes under /worker
        task = json.dumps(serialize_task(cls.CURRENT_TASK))
        for node_id, node_meta_data in cls.CONNECTED_NODES_METADATA.items():
            await socket_connection.emit(
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
    async def assign_reduce_keys(cls, socket_connection: socketio.Namespace) -> int:
        node_reduce_map = (
            cls.MASTER_NODE_HANDLER.assign_reduce_key_to_workers_round_robin(
                cls.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK
            )
        )
        number_of_nodes_assigned_reduce_keys = 0
        for node_id, node_meta_data in cls.CONNECTED_NODES_METADATA.items():
            await socket_connection.emit(
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
            number_of_nodes_assigned_reduce_keys += 1
        return number_of_nodes_assigned_reduce_keys

    @classmethod
    def insert_partial_result(cls, result: dict):
        cls.MASTER_NODE_HANDLER.aggregate_reduced_data(result)

    @classmethod
    async def trigger_task_queue(cls, socket_connection: socketio.Namespace):
        async for data in TaskQueueSingleton.dequeue():
            deserialized_task = deserialize_task(json.loads(data))
            MasterAPIInterface.RESULTS[deserialized_task.job_id] = {
                "start_time": datetime.datetime.now().timestamp()
            }
            await MasterAPIInterface.add_and_distribute_task(
                task=deserialized_task,
                socket_connection=socket_connection,
            )
            MasterAPIInterface.TASK_COMPLETE.clear()
            await MasterAPIInterface.TASK_COMPLETE.wait()

    @staticmethod
    async def add_task(task: str) -> None:
        await TaskQueueSingleton.enqueue(task)

    @classmethod
    def fetch_result(cls, job_id: str) -> dict:
        return MasterAPIInterface.RESULTS.get(job_id)
