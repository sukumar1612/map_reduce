import json
import tempfile
from typing import List, Union

import websockets

from services.factory import log_factory, worker_factory
from services.models import ReduceKeyWithIP, WebSocketMessage, WorkerTask
from services.worker_services.worker_class import WorkerNode


class WorkerAPIInterface:
    CSV_FILE = None
    CURRENT_JOB: Union[WorkerNode, None] = None
    OTHER_WORKER_NODE_IP: Union[List[str], None] = None
    LOG = None

    @classmethod
    def clear_all_prev_class_values(cls):
        if cls.CSV_FILE is not None:
            cls.CSV_FILE.close()
        cls.CSV_FILE = None
        cls.CURRENT_JOB = None
        cls.OTHER_WORKER_NODE_IP = None
        cls.LOG = None

    @classmethod
    def build_csv_file_from_chunks(cls, chunk: bytes):
        if cls.CSV_FILE is None:
            cls.CSV_FILE = tempfile.NamedTemporaryFile()
        cls.CSV_FILE.write(chunk)

    @classmethod
    def add_new_task(cls, task: WorkerTask) -> None:
        cls.LOG = log_factory(__name__, file_name=f"../logs/worker_{task.node_id}.log")
        cls.LOG.info(f"app : add new task -> {task}")
        task.file_name = cls.CSV_FILE.name
        cls.CURRENT_JOB = worker_factory(task=task)
        cls.LOG.info(f"app : add new task -> {task}")

    @classmethod
    def perform_mapping_and_return_distinct_keys(cls) -> list:
        cls.CURRENT_JOB.perform_mapping()
        cls.LOG.info(
            f"app : distinct keys list -> {cls.CURRENT_JOB.get_list_of_distinct_keys()}"
        )
        return cls.CURRENT_JOB.get_list_of_distinct_keys()

    @classmethod
    def assign_reduce_keys_to_node(cls, message: ReduceKeyWithIP) -> None:
        cls.CURRENT_JOB.assign_keys_to_be_reduced(message.key_list)
        cls.OTHER_WORKER_NODE_IP = message.other_worker_node_ip
        cls.LOG.info(
            f"app : keys assigned to node for reduction -> {cls.CURRENT_JOB.get_keys_to_be_reduced()}"
        )
        cls.LOG.info(
            f"app : list of other worker node ip -> {cls.OTHER_WORKER_NODE_IP}"
        )

    @classmethod
    def get_value_for_keys(cls, key_list: list) -> dict:
        data = {}
        for key in key_list:
            data[key] = cls.CURRENT_JOB.get_data_related_to_key(key)

        cls.LOG.info(f"app : data fetched for other nodes -> {data}")
        return data

    @classmethod
    async def shuffle(cls):
        for ip in cls.OTHER_WORKER_NODE_IP:
            async with websockets.connect(ip, timeout=40) as websocket:
                await websocket.send(
                    WebSocketMessage(
                        event="get_value_for_keys",
                        body={"key_list": cls.CURRENT_JOB.get_keys_to_be_reduced()},
                    ).json()
                )
                data = json.loads(await websocket.recv())
                cls.LOG.info(f"app : data transferred from {ip} -> {data}")
                for key in cls.CURRENT_JOB.get_keys_to_be_reduced():
                    cls.CURRENT_JOB.insert_data_from_other_workers(key, data[key])
                    cls.LOG.info(
                        f"app : aggregation of result after adding other node result -> {cls.CURRENT_JOB.get_data_related_to_key(key)}"
                    )

    @classmethod
    def perform_reduce_and_get_final_results(cls) -> dict:
        cls.CURRENT_JOB.perform_reduce()
        cls.LOG.info(f"app : reducer result -> {cls.CURRENT_JOB.get_reduced_result()}")
        return cls.CURRENT_JOB.get_reduced_result()
