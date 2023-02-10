import tempfile
from typing import Callable, Dict, List

import dill
import pandas as pd
from map_and_reduce_functions import MapFunction, ReduceFunction

from services.map_execution import MapAndShuffle, Reducer


class MasterNode:
    def __init__(self, data: pd.DataFrame, number_of_slave_nodes: int):
        self.__data = data
        self.__number_of_slave_nodes = number_of_slave_nodes
        self.__temporary_files_list = []
        self.__distinct_map_keys = set()
        self.__map_of_reduce_function_to_slave = {
            i: [] for i in range(0, number_of_slave_nodes)
        }
        self.__final_result = {}

    def split_data_for_each_slave_node(self) -> None:
        split_size = int(len(self.__data) / self.__number_of_slave_nodes)
        splits = [
            self.__data[split - split_size : split]
            if split != split_size * self.__number_of_slave_nodes
            else self.__data[
                split_size * self.__number_of_slave_nodes
                - split_size : len(self.__data)
            ]
            for split in range(0, len(self.__data) + 1, split_size)
        ]
        current_index = 0
        for split in splits:
            if current_index == 0:
                current_index += 1
                continue

            temp = tempfile.NamedTemporaryFile(prefix=f"file_{current_index - 1}_")
            split.to_csv(temp.name, index=False)
            current_index += 1
            self.__temporary_files_list.append(temp)
            temp.seek(0)

    def print_values_of_temporary_files(self):
        for file in self.__temporary_files_list:
            print(file.name)
            print(pd.read_csv(file.name))
            print("\n\n--------|||||-----------\n\n")
            file.seek(0)

    def create_task_object_for_slave_nodes(
        self, MapFunction: Callable, ReduceFunction: Callable
    ) -> List[Dict[str, str]]:
        return [
            {
                "file_name": file.name,
                "map_function": dill.dumps(MapFunction),
                "reduce_function": dill.dumps(ReduceFunction),
            }
            for file in self.__temporary_files_list
        ]

    def insert_map_keys_from_each_slave(self, map_keys: list):
        self.__distinct_map_keys = self.__distinct_map_keys.union(set(map_keys))

    def assign_reduce_key_to_slaves(self):
        for node, key in enumerate(list(self.__distinct_map_keys)):
            self.__map_of_reduce_function_to_slave[
                node % self.__number_of_slave_nodes
            ].append(key)

    def get_list_of_reduce_keys(self, node_number: int) -> list:
        return self.__map_of_reduce_function_to_slave[node_number]

    def final_result_aggregation(self, slave_result: dict):
        self.__final_result.update(slave_result)

    def get_final_results(self) -> dict:
        return self.__final_result


class SlaveNode:
    def __init__(self, task_: dict, node_number: int):
        self.__mapper_class = MapAndShuffle(
            pd.read_csv(task_["file_name"]).to_dict("records")
        )
        self.__reducer_class = Reducer()
        self.__map_function = task_["map_function"]
        self.__reduce_function = task_["reduce_function"]
        self.__keys_to_be_reduced = []
        self.node_number = node_number

    def get_map(self):
        return self.__mapper_class.get_map()

    def perform_mapping(self) -> None:
        self.__mapper_class.map_function_on_data(self.__map_function)

    def get_distinct_keys(self) -> list:
        return self.__mapper_class.keys()

    def get_data_related_to_key(self, key: str) -> list:
        return self.__mapper_class.get_data_for_key_in_map(key)

    def insert_data_from_other_slaves(self, key: str, value: list):
        self.__mapper_class.add_data_from_other_sources_to_map(key, value)

    def assign_keys_to_be_reduced(self, keys_to_be_reduced: list):
        self.__keys_to_be_reduced = keys_to_be_reduced

    def get_keys_to_be_reduced(self) -> list:
        return self.__keys_to_be_reduced

    def perform_reduce(self):
        for key in self.__keys_to_be_reduced:
            self.__reducer_class.reduced_function_on_data(
                reduce_function_as_a_string=self.__reduce_function,
                key=key,
                values=self.__mapper_class.get_data_for_key_in_map(key),
            )

    def get_reduced_result(self) -> dict:
        return self.__reducer_class.reduced_data


if __name__ == "__main__":
    """Input Task"""
    task = {
        "file_name": "random_data.csv",
        "map_function": dill.dumps(MapFunction),
        "reduce_function": dill.dumps(ReduceFunction),
    }
    dataframe = pd.read_csv("random_data.csv")
    """ Create Tasks for Nodes """
    number_of_slave_nodes = 3
    master = MasterNode(data=dataframe, number_of_slave_nodes=number_of_slave_nodes)
    master.split_data_for_each_slave_node()
    tasks = master.create_task_object_for_slave_nodes(
        MapFunction=MapFunction, ReduceFunction=ReduceFunction
    )

    master.print_values_of_temporary_files()
    """ send tasks to each slave Node """
    slaves = [
        SlaveNode(task, node_number=task_number)
        for task_number, task in enumerate(tasks)
    ]
    for slave in slaves:
        slave.perform_mapping()

    """ send list of distinct keys from slave to master """
    for slave in slaves:
        master.insert_map_keys_from_each_slave(slave.get_distinct_keys())

    """ master assigns key data to reduce to each node """
    master.assign_reduce_key_to_slaves()
    for node_number, slave in enumerate(slaves):
        slave.assign_keys_to_be_reduced(
            master.get_list_of_reduce_keys(node_number=node_number)
        )

    """ fetch all required data from each of the other slaves for reducing (shuffling) """
    for node_number_1, slave_1 in enumerate(slaves):
        for node_number_2, slave_2 in enumerate(slaves):
            if node_number_1 != node_number_2:
                for key in slave_1.get_keys_to_be_reduced():
                    slave_1.insert_data_from_other_slaves(
                        key, slave_2.get_data_related_to_key(key)
                    )

    """ perform reduce """
    for slave in slaves:
        slave.perform_reduce()

    """return result from slave to master"""
    for slave in slaves:
        master.final_result_aggregation(slave.get_reduced_result())

    print(master.get_final_results())
