import pprint

import dill

from services.models import Task
from services.worker.mapper_and_reducer import mapper_and_reducer_factory
from tests.test_map_reduce_functions import MapFunction, ReduceFunction

if __name__ == "__main__":
    mapper = mapper_and_reducer_factory(file_name="random_data_1.csv")
    mapper.map_data(
        task=Task(
            mapper_function=dill.dumps(MapFunction),
            reducer_function=dill.dumps(ReduceFunction),
        )
    )
    mapper.add_reduce_keys(["apple", "orange", "banana"])
    mapper.reduce_data(
        task=Task(
            mapper_function=dill.dumps(MapFunction),
            reducer_function=dill.dumps(ReduceFunction),
        )
    )
    pprint.pprint(mapper.groups_after_reducing)
