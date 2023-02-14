import json
import os
import tempfile
from pathlib import Path
from typing import List, Union

import dill
import pandas as pd
import websockets

from services.exceptions import InvalidSplitSize
from services.factory import master_factory
from services.master_services.master_class import MasterNode
from services.models import FileModel, WebSocketMessage, serialize_file_model, WorkerTask, Task, serialize_task
from unit_tests.simulation.map_and_reduce_functions import MapFunction, ReduceFunction

CHUNK_SIZE = 1024 * 100


class MasterAPIInterface:
    CURRENT_JOB: Union[MasterNode, None] = None
    WORKER_NODE_CONNECTIONS: dict = {}
    NUMBER_OF_NODES_IN_USE: int = 0
    NUMBER_OF_NODES_CONNECTED: int = 0
    LOG = None

    @staticmethod
    def create_chunks(
            initialization_data: pd.DataFrame, worker_node_count: int
    ) -> List[pd.DataFrame]:
        split_size = int(len(initialization_data) / worker_node_count)
        if split_size <= 0:
            raise InvalidSplitSize
        splits = [
            initialization_data[split - split_size: split]
            if split != split_size * worker_node_count
            else initialization_data[
                 split_size * worker_node_count - split_size: len(initialization_data)
                 ]
            for split in range(0, len(initialization_data) + 1, split_size)
        ]
        while len(splits) > worker_node_count + 1:
            splits.pop()
        return splits

    @staticmethod
    def create_chunks_as_temporary_file(
            initialization_data: pd.DataFrame, worker_node_count: int
    ) -> list:
        chunks = MasterAPIInterface.create_chunks(
            initialization_data, worker_node_count
        )
        list_of_temporary_files = []
        for index, chunk in enumerate(chunks[1:]):
            temp = tempfile.NamedTemporaryFile(prefix=f"file_{index}_")
            chunk.to_csv(temp.name, index=False)
            list_of_temporary_files.append(temp)
            temp.seek(0)
        return list_of_temporary_files

    @staticmethod
    def file_chunking(file):
        chunks = []
        temp_chunk = file.read(CHUNK_SIZE)
        while temp_chunk:
            chunks.append(temp_chunk)
            temp_chunk = file.read(CHUNK_SIZE)

        for index, chunk in enumerate(chunks):
            file_chunk = json.dumps(serialize_file_model(
                FileModel(chunk=chunk, chunk_index=index, completed=False)
            ))
            print(f"sent chunk: {index + 1}")
            # print(file_chunk)
            yield file_chunk

    @classmethod
    def add_new_worker_node(cls, sid) -> int:
        cls.WORKER_NODE_CONNECTIONS[cls.NUMBER_OF_NODES_CONNECTED] = sid
        cls.NUMBER_OF_NODES_CONNECTED += 1
        print(f"connected: worker with id = {cls.NUMBER_OF_NODES_CONNECTED - 1}")
        print(f"number of connected nodes =  {cls.NUMBER_OF_NODES_CONNECTED}")
        return cls.NUMBER_OF_NODES_CONNECTED - 1

    @classmethod
    def initialization_sequence(cls, sio):
        cls.NUMBER_OF_NODES_IN_USE = cls.NUMBER_OF_NODES_CONNECTED
        print(cls.NUMBER_OF_NODES_IN_USE)
        temp_file_list = cls.create_chunks_as_temporary_file(
            initialization_data=pd.read_csv(
                os.path.join(
                    Path(os.path.dirname(__file__)).parent.parent,
                    "unit_tests/simulation/random_data_1.csv",
                )
            ),
            worker_node_count=cls.NUMBER_OF_NODES_IN_USE,
        )
        for key, connection in cls.WORKER_NODE_CONNECTIONS.items():
            for file_chunk in cls.file_chunking(temp_file_list[key]):
                sio.emit('file_initialization', {
                    "file": file_chunk
                }, room=connection, namespace='/worker')

            sio.emit('file_initialization', {
                "file": json.dumps(serialize_file_model(
                    FileModel(chunk="", chunk_index=-1, completed=True)
                ))
            }, room=connection, namespace='/worker')
            print(f"sent file to node {key} successfully")

    @classmethod
    def distribute_task_and_perform_map_reduce(cls, sio_master, task=None):
        cls.CURRENT_JOB = master_factory(
            task=Task(
                map_function=dill.dumps(MapFunction),
                reduce_function=dill.dumps(ReduceFunction)
            ),
            data_file_name=os.path.join(
                Path(os.path.dirname(__file__)).parent.parent,
                "unit_tests/simulation/random_data_1.csv",
            ),
            number_of_worker_nodes=cls.NUMBER_OF_NODES_CONNECTED,
        )
        for key, sio_id in MasterAPIInterface.WORKER_NODE_CONNECTIONS.items():
            task = json.dumps(serialize_task(WorkerTask(
                map_function=dill.dumps(MapFunction),
                reduce_function=dill.dumps(ReduceFunction),
                node_id=key
            )))
            sio_master.emit(
                'perform_map_reduce', {
                    "task": task
                }, room=sio_id, namespace='/worker'
            )
            print(f"map_reduce_done for node {key}")

    @classmethod
    def insert_map_result_data(cls, map_result: list):
        cls.CURRENT_JOB.insert_distinct_keys_from_each_worker(map_result)
