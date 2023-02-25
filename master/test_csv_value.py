import marshal
import pprint

import dill

from services.models import Task
from services.worker.mapper_and_reducer import mapper_and_reducer_factory
from tests.test_map_reduce_functions import (MapFunction, MapFunction1,
                                             ReduceFunction, ReduceFunction1)

if __name__ == "__main__":
    mapper = mapper_and_reducer_factory(file_name="random_data_1.csv")
    mapper.map_data(
        task=Task(
            mapper_function=marshal.dumps(MapFunction.__code__),
            reducer_function=marshal.dumps(ReduceFunction.__code__),
        )
    )
    mapper.add_reduce_keys(["apple", "orange", "banana"])
    mapper.reduce_data(
        task=Task(
            mapper_function=marshal.dumps(MapFunction.__code__),
            reducer_function=marshal.dumps(ReduceFunction.__code__),
        )
    )
    pprint.pprint(mapper.groups_after_reducing)

    mapper = mapper_and_reducer_factory(file_name="random_data_1.csv")
    mapper.map_data(
        task=Task(
            mapper_function=marshal.dumps(MapFunction1.__code__),
            reducer_function=marshal.dumps(ReduceFunction1.__code__),
        )
    )
    mapper.add_reduce_keys(["apple", "orange", "banana"])
    mapper.reduce_data(
        task=Task(
            mapper_function=marshal.dumps(MapFunction1.__code__),
            reducer_function=marshal.dumps(ReduceFunction1.__code__),
        )
    )
    pprint.pprint(mapper.groups_after_reducing)
