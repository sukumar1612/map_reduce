import tempfile
from typing import List

import pandas as pd

from exceptions import InvalidSplitSize
from services.models import Task


class MasterNode:
    def __init__(self, job_id: str, data: pd.DataFrame, number_of_worker_nodes: int):
        self._temporary_files_list = []
        self._final_result = {}
        self._map_of_task_assigned_to_worker_node = {}
        self._distinct_map_keys = set()
        self._data = data
        self._number_of_worker_nodes = number_of_worker_nodes
        self._map_of_reduce_function_to_worker = {
            i: [] for i in range(0, number_of_worker_nodes)
        }
        self._id = job_id

    @property
    def job_id(self) -> str:
        return self._id

    def create_chunks(self) -> List[pd.DataFrame]:
        split_size = int(len(self._data) / self._number_of_worker_nodes)
        if split_size <= 0:
            raise InvalidSplitSize
        splits = [
            self._data[split - split_size : split]
            if split != split_size * self._number_of_worker_nodes
            else self._data[
                split_size * self._number_of_worker_nodes - split_size : len(self._data)
            ]
            for split in range(0, len(self._data) + 1, split_size)
        ]
        while len(splits) > self._number_of_worker_nodes + 1:
            splits.pop()
        return splits

    def create_chunks_of_data_for_each_worker_node(self) -> None:
        chunks = self.create_chunks()
        for index, chunk in enumerate(chunks[1:]):
            temp = tempfile.NamedTemporaryFile(prefix=f"file_{index}_")
            chunk.to_csv(temp.name, index=False)
            self._temporary_files_list.append(temp)
            temp.seek(0)

    def create_tasks_for_all_worker_nodes(self, task: Task) -> List[Task]:
        result = []
        for index, file in enumerate(self._temporary_files_list):
            worker_node_task = Task(
                job_id=task.job_id,
                file_name=file.name,
                map_function=task.map_function,
                reduce_function=task.reduce_function,
            )
            self._map_of_task_assigned_to_worker_node[index] = worker_node_task
            result.append(worker_node_task)
        return result

    def insert_distinct_keys_from_each_worker(self, distinct_keys: list):
        self._distinct_map_keys = self._distinct_map_keys.union(set(distinct_keys))

    def assign_reduce_key_to_workers_round_robin(self):
        for node, key in enumerate(list(self._distinct_map_keys)):
            self._map_of_reduce_function_to_worker[
                node % self._number_of_worker_nodes
            ].append(key)

    def get_list_of_reduce_keys(self, node_id: int) -> list:
        return self._map_of_reduce_function_to_worker[node_id]

    def final_result_aggregation(self, worker_result: dict):
        self._final_result.update(worker_result)

    def get_final_results(self) -> dict:
        for temporary_file in self._temporary_files_list:
            temporary_file.close()
        return self._final_result


def master_factory(
    task: Task,
    data_file_name: str,
    number_of_worker_nodes: int,
    master_class=MasterNode,
):
    dataframe = pd.read_csv(data_file_name)
    return master_class(
        job_id=task.job_id,
        data=dataframe,
        number_of_worker_nodes=number_of_worker_nodes,
    )
