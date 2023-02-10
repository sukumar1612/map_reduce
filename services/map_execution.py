from functools import reduce
from typing import List

import dill


class Structure:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class FunctionGenerators:
    @staticmethod
    def convert_dict_to_object(dictionary: dict) -> Structure:
        return Structure(**dictionary)

    @staticmethod
    def convert_string_to_function(function_binary_for_dill: str):
        return dill.loads(function_binary_for_dill)


class MapAndShuffle(FunctionGenerators):
    def __init__(self, record_list: List[dict]):
        self.record_list = record_list
        self.mapped_groups = {}

    def get_map(self):
        return self.mapped_groups

    def map_function_on_data(self, map_function_as_string: str) -> None:
        map_function = self.convert_string_to_function(map_function_as_string)
        self.mapped_groups = {}
        for data in self.record_list:
            key, value = map_function(self.convert_dict_to_object(data))
            if key not in self.mapped_groups:
                self.mapped_groups[key] = []
            self.mapped_groups[key].append(value)

    def get_data_for_key_in_map(self, key: str) -> list:
        return self.mapped_groups[key]

    def keys(self) -> list:
        return list(self.mapped_groups.keys())

    def add_data_from_other_sources_to_map(self, key: str, value: list):
        if key in self.mapped_groups:
            self.mapped_groups[key].extend(value)
            return

        self.mapped_groups[key] = value


class Reducer(FunctionGenerators):
    def __init__(self):
        self.reduced_data = {}

    def reduced_function_on_data(
        self, reduce_function_as_a_string: str, key: str, values: list
    ) -> None:
        self.reduced_data[key] = reduce(
            self.convert_string_to_function(reduce_function_as_a_string),
            values,
        )

    def get_reduced_data(self, key):
        return self.reduced_data[key]
