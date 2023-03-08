import asyncio
import base64
import pickle
import tempfile
from io import TextIOWrapper
from typing import Any, Dict, Union

import socketio

from common.logger_module import get_logger
from common.models import Task
from worker.services.mapper_and_reducer import (MapperAndReducer,
                                                mapper_and_reducer_factory)

LOG = get_logger(__name__)


class TaskTracker:
    RECORD_FILE: Union[TextIOWrapper, None] = None
    CURRENT_TASK: Union[Task, None] = None
    WORKER_ID: Union[int, None] = None
    MAP_REDUCE_HANDLER: Union[MapperAndReducer, None] = None

    SHARED_MAP_VALUE: dict = None

    @classmethod
    def clear_map(cls):
        cls.SHARED_MAP_VALUE.clear()

    @classmethod
    def update_map_list(cls, new_map_values: dict):
        cls.clear_map()
        cls.SHARED_MAP_VALUE.update(new_map_values)

    @classmethod
    def reset_state(cls):
        cls.RECORD_FILE.close()
        cls.RECORD_FILE = None
        cls.MAP_REDUCE_HANDLER = None
        cls.CURRENT_TASK = None
        LOG.debug("reset all state variables")

    @classmethod
    def prepare_for_next_task(cls):
        cls.MAP_REDUCE_HANDLER.reset_state()
        cls.MAP_REDUCE_HANDLER = None
        cls.CURRENT_TASK = None
        LOG.debug("prepare for next task")

    @classmethod
    def build_csv_file_from_chunks(cls, chunk: bytes):
        if cls.RECORD_FILE is None:
            cls.RECORD_FILE = tempfile.NamedTemporaryFile()
        cls.RECORD_FILE.write(chunk)

    @classmethod
    def add_new_task(cls, task: Task) -> None:
        cls.CURRENT_TASK = task
        if cls.MAP_REDUCE_HANDLER is None:
            cls.RECORD_FILE.seek(0)
            cls.MAP_REDUCE_HANDLER = mapper_and_reducer_factory(
                file_name=cls.RECORD_FILE.name
            )

    @classmethod
    def perform_mapping_and_return_distinct_keys(cls) -> list:
        cls.MAP_REDUCE_HANDLER.map_data(task=cls.CURRENT_TASK)
        cls.update_map_list(cls.MAP_REDUCE_HANDLER.get_mapped_groups())
        return cls.MAP_REDUCE_HANDLER.fetch_distinct_keys()

    @classmethod
    def insert_reduce_keys_for_current_node(cls, key_list: list) -> None:
        cls.MAP_REDUCE_HANDLER.add_reduce_keys(key_list=key_list)

    @classmethod
    def shuffle(cls, connected_nodes_ip: Dict[int, str]):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        asynchronous_function_calls = asyncio.gather(
            *[
                loop.create_task(receive_other_node_map_data(cls, ip))
                for _, ip in connected_nodes_ip.items()
            ]
        )
        connection_threads_output = loop.run_until_complete(asynchronous_function_calls)
        loop.close()

        for node_id in range(len(connected_nodes_ip.keys())):
            cls.MAP_REDUCE_HANDLER.extend_map(connection_threads_output[node_id])

    @classmethod
    def perform_reduce_and_get_final_results(cls) -> dict:
        cls.MAP_REDUCE_HANDLER.reduce_data(task=cls.CURRENT_TASK)
        return cls.MAP_REDUCE_HANDLER.get_reduced_groups()

    @classmethod
    def fetch_mapped_value_for_specific_key(cls, key_list: list) -> dict:
        return {key: cls.SHARED_MAP_VALUE.get(key, None) for key in key_list}


async def receive_other_node_map_data(api_interface: TaskTracker, ip: str) -> Any:
    socket_connection = socketio.AsyncClient()
    file = tempfile.NamedTemporaryFile()

    @socket_connection.on("receive_key_value", namespace="/p2p")
    async def on_receive_key_value(message_body: dict):
        if message_body["completed"] is True:
            file.seek(0)
            await socket_connection.disconnect()
        else:
            file.write(base64.b64decode(message_body["chunk"].encode("ascii")))

    await socket_connection.connect(ip)
    await socket_connection.emit(
        "request_value_for_list_of_keys",
        {"key_list": api_interface.MAP_REDUCE_HANDLER.reduce_keys},
        namespace="/p2p",
    )
    await socket_connection.wait()
    constructed_object = pickle.load(file)
    file.close()
    return constructed_object
