import pprint

import dill
import pandas as pd

from services.master_worker_service.master_class import (MasterNode,
                                                         master_factory)
from services.master_worker_service.worker_class import (WorkerNode,
                                                         worker_factory)
from services.models import Task
from unit_tests.simulation.map_and_reduce_functions import (MapFunction,
                                                            ReduceFunction)


class MasterNodeTesting(MasterNode):
    def print_values_of_temporary_files(self):
        for file in self._temporary_files_list:
            print(file.name)
            print(pd.read_csv(file.name))
            print("\n\n--------|||||-----------\n\n")
            file.seek(0)


if __name__ == "__main__":
    """Input Task"""
    task = Task(
        file_name="random_data.csv",
        map_function=dill.dumps(MapFunction),
        reduce_function=dill.dumps(ReduceFunction),
    )
    """ Create Tasks for Nodes """
    number_of_worker_nodes = 3
    master = master_factory(
        task=task,
        data_file_name="random_data.csv",
        number_of_worker_nodes=number_of_worker_nodes,
        master_class=MasterNodeTesting,
    )
    master.create_chunks_of_data_for_each_worker_node()
    tasks = master.create_tasks_for_all_worker_nodes(task=task)

    master.print_values_of_temporary_files()
    """ send tasks to each worker Node """
    pprint.pprint(task.dict())
    pprint.pprint([task.dict() for task in tasks])
    workers = [
        worker_factory(task=task)
        for task_number, task in enumerate(tasks)
    ]
    for worker in workers:
        worker.perform_mapping()

    """ send list of distinct keys from worker to master """
    for worker in workers:
        master.insert_distinct_keys_from_each_worker(
            distinct_keys=worker.get_list_of_distinct_keys()
        )

    """ master assigns key data to reduce to each node """
    master.assign_reduce_key_to_workers_round_robin()
    for node_number, worker in enumerate(workers):
        worker.assign_keys_to_be_reduced(
            master.get_list_of_reduce_keys(node_id=node_number)
        )

    """ fetch all required data from each of the other workers for reducing (shuffling) """
    for node_number_1, worker_1 in enumerate(workers):
        for node_number_2, worker_2 in enumerate(workers):
            if node_number_1 != node_number_2:
                for key in worker_1.get_keys_to_be_reduced():
                    worker_1.insert_data_from_other_workers(
                        key, worker_2.get_data_related_to_key(key)
                    )

    """ perform reduce """
    for worker in workers:
        worker.perform_reduce()

    """return result from worker to master"""
    for worker in workers:
        master.final_result_aggregation(worker.get_reduced_result())

    print(master.get_final_results())
