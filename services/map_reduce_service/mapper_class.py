from typing import List, Union

from services.map_reduce_service.base_class import FunctionGenerators
from services.models import Task


class MapAndShuffle(FunctionGenerators):
    def __init__(self, record_list: List[dict]):
        self.__record_list = record_list
        self.__mapped_groups = {}

    def get_map(self):
        return self.__mapped_groups

    def map_function_on_data(self, task: Task) -> None:
        map_function = self.de_serialize_function(task.map_function)
        self.__mapped_groups = {}
        for data in self.__record_list:
            key, value = map_function(self.convert_dict_to_object(data))
            if key not in self.__mapped_groups:
                self.__mapped_groups[key] = []
            self.__mapped_groups[key].append(value)

    def get_data_for_key_in_map(self, key: str) -> Union[list, None]:
        return self.__mapped_groups.get(key, None)

    def keys(self) -> list:
        return list(self.__mapped_groups.keys())

    def add_data_from_other_sources_to_map(self, key: str, value: list):
        if value is None:
            return

        if self.__mapped_groups.get(key, None) is not None:
            self.__mapped_groups[key].extend(value)
            return

        self.__mapped_groups[key] = value
