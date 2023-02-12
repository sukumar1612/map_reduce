import pandas as pd

from services.map_reduce_service.mapper_class import MapAndShuffle
from services.map_reduce_service.reducer_class import Reducer
from services.models import Task, WorkerTask


class WorkerNode:
    def __init__(
        self,
        mapper_class: MapAndShuffle,
        reducer_class: Reducer,
        task: WorkerTask,
    ):
        self.__mapper_class = mapper_class
        self.__reducer_class = reducer_class
        self.__task = task
        self.__keys_to_be_reduced = []
        self.node_id = task.node_id

    def get_map(self):
        return self.__mapper_class.get_map()

    def perform_mapping(self) -> None:
        self.__mapper_class.map_function_on_data(self.__task)
        print(self.__mapper_class.get_map())

    def get_list_of_distinct_keys(self) -> list:
        return self.__mapper_class.keys()

    def get_data_related_to_key(self, key: str) -> list:
        return self.__mapper_class.get_data_for_key_in_map(key)

    def insert_data_from_other_workers(self, key: str, value: list):
        self.__mapper_class.add_data_from_other_sources_to_map(key, value)

    def assign_keys_to_be_reduced(self, keys_to_be_reduced: list):
        self.__keys_to_be_reduced = keys_to_be_reduced

    def get_keys_to_be_reduced(self) -> list:
        return self.__keys_to_be_reduced

    def perform_reduce(self):
        for key in self.__keys_to_be_reduced:
            self.__reducer_class.reduced_function_on_data(
                task=self.__task,
                key=key,
                values=self.__mapper_class.get_data_for_key_in_map(key),
            )

    def get_reduced_result(self) -> dict:
        return self.__reducer_class.get_reduced_data_all()


def worker_factory(task: WorkerTask) -> WorkerNode:
    mapper_class = MapAndShuffle(pd.read_csv(task.file_name).to_dict("records"))
    reducer_class = Reducer()
    return WorkerNode(mapper_class, reducer_class, task)
