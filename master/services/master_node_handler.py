import json
import tempfile
from io import TextIOWrapper
from typing import Any, Dict, Generator, List, Union

import numpy as np
import pandas as pd

from common.models import FileModel, Task, serialize_file_model


class MasterNode:
    CHUNK_SIZE = 1024 * 100

    def __init__(self, records: pd.DataFrame):
        self.records = records
        self.distinct_keys = set()
        self.reduced_data = {}

    @staticmethod
    def create_chunks_of_dataframe(
        initialization_data: pd.DataFrame, worker_node_count: int
    ) -> List[pd.DataFrame]:
        return np.array_split(initialization_data, worker_node_count)

    def create_dataframe_chunks_as_temporary_file(self, worker_node_count: int) -> list:
        chunks = MasterNode.create_chunks_of_dataframe(self.records, worker_node_count)
        list_of_temporary_files = []
        for index, chunk in enumerate(chunks):
            temp = tempfile.NamedTemporaryFile(prefix=f"file_{index}_")
            chunk.to_csv(temp.name, index=False)
            list_of_temporary_files.append(temp)
            temp.seek(0)
        return list_of_temporary_files

    @staticmethod
    def file_chunking(file: TextIOWrapper):
        chunks = []
        temp_chunk = file.read(MasterNode.CHUNK_SIZE)
        while temp_chunk:
            chunks.append(temp_chunk)
            temp_chunk = file.read(MasterNode.CHUNK_SIZE)

        for index, chunk in enumerate(chunks):
            file_chunk = json.dumps(
                serialize_file_model(
                    FileModel(chunk=chunk, chunk_index=index, completed=False)
                )
            )
            print(f"sent chunk: {index + 1}")
            yield file_chunk

    def reset_state(self) -> None:
        self.distinct_keys = set()
        self.reduced_data = {}

    def insert_mapped_keys(self, distinct_keys: list) -> None:
        self.distinct_keys = self.distinct_keys.union(set(distinct_keys))

    def assign_reduce_key_to_workers_round_robin(
        self, worker_node_count: int
    ) -> Dict[int, list]:
        map_reduce_key_to_node_id = {_id: [] for _id in range(worker_node_count)}
        for index, key in enumerate(list(self.distinct_keys)):
            map_reduce_key_to_node_id[index % worker_node_count].append(key)
        return map_reduce_key_to_node_id

    def aggregate_reduced_data(self, result: dict) -> None:
        self.reduced_data.update(result)

    def get_final_reduced_data(self) -> dict:
        return self.reduced_data


def master_node_factory(file_name: str):
    return MasterNode(records=pd.read_csv(file_name))
