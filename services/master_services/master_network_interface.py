import os
import tempfile
from pathlib import Path
from typing import List, Union

import pandas as pd
import websockets

from services.exceptions import InvalidSplitSize
from services.master_services.master_class import MasterNode
from services.models import FileModel, WebSocketMessage, serialize_file_model

CHUNK_SIZE = 1024 * 100


class MasterAPIInterface:
    CURRENT_JOB: Union[MasterNode, None] = None
    ALL_WORKER_NODE_CONNECTIONS: list = []
    NUMBER_OF_NODES_IN_USE: int = 0
    LOG = None

    @staticmethod
    def create_chunks(
        initialization_data: pd.DataFrame, worker_node_count: int
    ) -> List[pd.DataFrame]:
        split_size = int(len(initialization_data) / worker_node_count)
        if split_size <= 0:
            raise InvalidSplitSize
        splits = [
            initialization_data[split - split_size : split]
            if split != split_size * worker_node_count
            else initialization_data[
                split_size * worker_node_count - split_size : len(initialization_data)
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
            file_chunk = WebSocketMessage(
                event="file_stream",
                body=serialize_file_model(
                    FileModel(chunk=chunk, chunk_index=index, completed=False)
                ),
            ).json()
            print(f"sent chunk: {index + 1}")
            print(file_chunk)
            yield file_chunk

    @classmethod
    def add_new_worker_node(cls, ip: str):
        print(f"connected: worker with IP = {ip}")
        cls.ALL_WORKER_NODE_CONNECTIONS.append(ip)

    @classmethod
    async def initialization_sequence(cls):
        cls.NUMBER_OF_NODES_IN_USE = len(cls.ALL_WORKER_NODE_CONNECTIONS)
        temp_file_list = cls.create_chunks_as_temporary_file(
            initialization_data=pd.read_csv(
                os.path.join(
                    Path(os.path.dirname(__file__)).parent.parent,
                    "unit_tests/simulation/random_data.csv",
                )
            ),
            worker_node_count=cls.NUMBER_OF_NODES_IN_USE,
        )
        for index, ip in enumerate(cls.ALL_WORKER_NODE_CONNECTIONS):
            async with websockets.connect(ip, timeout=40) as websocket:
                for file_chunk in cls.file_chunking(temp_file_list[index]):
                    await websocket.send(file_chunk)
                    response = await websocket.recv()
                    if response == "ACK":
                        print("Success")
                await websocket.send(
                    WebSocketMessage(
                        event="file_stream",
                        body=serialize_file_model(
                            FileModel(chunk="", chunk_index=-1, completed=True)
                        ),
                    ).json()
                )
                response = await websocket.recv()
                if response == "ACK":
                    print("Done")
