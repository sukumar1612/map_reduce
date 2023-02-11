from typing import Any, Union

from services.map_reduce_service.base_class import FunctionGenerators
from services.models import Task


class Reducer(FunctionGenerators):
    def __init__(self):
        self.__reduced_data = {}

    def reduced_function_on_data(self, task: Task, key: str, values: list) -> None:
        self.__reduced_data[key] = self.de_serialize_function(task.reduce_function)(
            values
        )

    def get_reduced_data(self, key) -> Union[Any, None]:
        return self.__reduced_data.get(key, None)

    def get_reduced_data_all(self):
        return self.__reduced_data
