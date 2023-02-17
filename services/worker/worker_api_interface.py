import pprint
import tempfile
import threading
from io import TextIOWrapper
from typing import Dict, Union

import socketio

from services.models import Task
from services.worker.mapper_and_reducer import (MapperAndReducer,
                                                mapper_and_reducer_factory)


class WorkerAPIInterface:
    RECORD_FILE: Union[TextIOWrapper, None] = None
    CURRENT_TASK: Union[Task, None] = None
    WORKER_ID: Union[int, None] = None
    MAP_REDUCE_HANDLER: Union[MapperAndReducer, None] = None

    @classmethod
    def reset_state(cls):
        cls.RECORD_FILE.close()
        cls.RECORD_FILE = None
        cls.MAP_REDUCE_HANDLER = None
        cls.CURRENT_TASK = None

    @classmethod
    def prepare_for_next_task(cls):
        cls.MAP_REDUCE_HANDLER.reset_state()
        cls.MAP_REDUCE_HANDLER = None
        cls.CURRENT_TASK = None

    @classmethod
    def build_csv_file_from_chunks(cls, chunk: bytes):
        if cls.RECORD_FILE is None:
            cls.RECORD_FILE = tempfile.NamedTemporaryFile()
        cls.RECORD_FILE.write(chunk)

    @classmethod
    def add_new_task(cls, task: Task) -> None:
        cls.CURRENT_TASK = task
        if cls.MAP_REDUCE_HANDLER is None:
            cls.MAP_REDUCE_HANDLER = mapper_and_reducer_factory(
                file_name=cls.RECORD_FILE.name
            )

    @classmethod
    def perform_mapping_and_return_distinct_keys(cls) -> list:
        cls.MAP_REDUCE_HANDLER.map_data(task=cls.CURRENT_TASK)
        return cls.MAP_REDUCE_HANDLER.fetch_distinct_keys()

    @classmethod
    def insert_reduce_keys_for_current_node(cls, key_list: list) -> None:
        cls.MAP_REDUCE_HANDLER.add_reduce_keys(key_list=key_list)

    @classmethod
    def shuffle(cls, connected_nodes_ip: Dict[int, str]):
        connection_threads = []
        pprint.pprint(connected_nodes_ip)
        for node_id, ip in connected_nodes_ip.items():
            # connection_threads.append(
            #     threading.Thread(target=connection_thread, args=(cls, ip)))
            # connection_threads[len(connection_threads) - 1].daemon = True
            # connection_threads[len(connection_threads) - 1].start()
            connection_thread(cls, ip)

        # for connection in connection_threads:
        #     connection.join()

    @classmethod
    def perform_reduce_and_get_final_results(cls) -> dict:
        cls.MAP_REDUCE_HANDLER.reduce_data(task=cls.CURRENT_TASK)
        return cls.MAP_REDUCE_HANDLER.get_reduced_groups()

    @classmethod
    def fetch_mapped_value_for_specific_key(cls, key_list: list) -> dict:
        return {
            key: cls.MAP_REDUCE_HANDLER.fetch_mapped_values_for_specific_key(key=key)
            for key in key_list
        }


def connection_thread(api_interface: WorkerAPIInterface, ip: str):
    socket_connection = socketio.Client()

    @socket_connection.on("receive_key_value", namespace="/p2p")
    def on_receive_key_value(message_body: dict):
        api_interface.MAP_REDUCE_HANDLER.extend_map(message_body)
        socket_connection.disconnect()

    socket_connection.connect(ip)
    socket_connection.emit(
        "request_value_for_list_of_keys",
        {"key_list": api_interface.MAP_REDUCE_HANDLER.reduce_keys},
        namespace="/p2p",
    )
    socket_connection.wait()
