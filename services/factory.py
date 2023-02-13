import logging

import pandas as pd

from services.map_reduce_service.mapper_class import MapAndShuffle
from services.map_reduce_service.reducer_class import Reducer
from services.master_services.master_class import MasterNode
from services.models import Task, WorkerTask
from services.worker_services.worker_class import WorkerNode


def master_factory(
    task: Task,
    data_file_name: str,
    number_of_worker_nodes: int,
    master_class=MasterNode,
):
    dataframe = pd.read_csv(data_file_name)
    return master_class(
        job_id=task.job_id,
        data=dataframe,
        number_of_worker_nodes=number_of_worker_nodes,
    )


def worker_factory(task: WorkerTask) -> WorkerNode:
    mapper_class = MapAndShuffle(pd.read_csv(task.file_name).to_dict("records"))
    reducer_class = Reducer()
    return WorkerNode(mapper_class, reducer_class, task)


def log_factory(name: str, file_name: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=file_name,
    )
    log = logging.getLogger(name)
    return log
