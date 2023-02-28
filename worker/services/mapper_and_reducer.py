import marshal
import types
from typing import Any, Dict, List, Union

import dill
import pandas as pd

from common.models import Task


class DictionaryToObject:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class MapperAndReducer:
    def __init__(self, csv_iterator: list):
        self.csv_iterator: list = csv_iterator
        self.groups_after_mapping: Dict[str, list] = {}
        self.groups_after_reducing: Dict[str, list] = {}
        self.reduce_keys: list = []

    def map_data(self, task: Task) -> None:
        mapper_function = types.FunctionType(
            marshal.loads(task.mapper_function), globals(), "MapFunc"
        )
        for chunk in self.csv_iterator:
            self.groups_after_mapping.update(mapper_function(chunk.to_dict("records")))

    def reduce_data(self, task: Task) -> None:
        reducer_function = types.FunctionType(
            marshal.loads(task.reducer_function), globals(), "ReduceFunc"
        )
        for key in self.reduce_keys:
            self.groups_after_reducing[key] = reducer_function(
                self.groups_after_mapping[key]
            )

    def extend_map(self, result: dict) -> None:
        for key, value in result.items():
            if key not in self.groups_after_mapping:
                self.groups_after_mapping[key] = value
                return
            self.groups_after_mapping[key].extend(value)

    def add_reduce_keys(self, key_list: list) -> None:
        self.reduce_keys = key_list

    def fetch_mapped_values_for_specific_key(self, key: str) -> Union[list, None]:
        return self.groups_after_mapping.get(key, None)

    def fetch_reduced_values_for_specific_key(self, key: str) -> Union[Any, None]:
        return self.groups_after_reducing.get(key, None)

    def get_mapped_groups(self) -> dict:
        return self.groups_after_mapping

    def get_reduced_groups(self) -> dict:
        return self.groups_after_reducing

    def fetch_distinct_keys(self) -> list:
        return list(self.groups_after_mapping.keys())

    def reset_state(self) -> None:
        self.groups_after_mapping = {}
        self.groups_after_reducing = {}
        self.reduce_keys = []


def mapper_and_reducer_factory(file_name: str) -> MapperAndReducer:
    return MapperAndReducer(csv_iterator=pd.read_csv(file_name, chunksize=100000))
