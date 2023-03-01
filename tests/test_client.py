import asyncio
import marshal

from client.events.event_handler import app
from client.services.state_manager import StateManager
from common.models import Task
from tests.test_map_reduce_functions import (MapFunction, MapFunction1,
                                             ReduceFunction, ReduceFunction1)

if __name__ == "__main__":
    task1 = Task(
        mapper_function=marshal.dumps(MapFunction.__code__),
        reducer_function=marshal.dumps(ReduceFunction.__code__),
    )
    task2 = Task(
        mapper_function=marshal.dumps(MapFunction1.__code__),
        reducer_function=marshal.dumps(ReduceFunction1.__code__),
    )
    StateManager.jobs = [task1, task2]
    asyncio.run(app.run())
