import base64
import binascii
import os
from typing import List, Optional, Union

from pydantic import BaseModel

""" common to all services """


class Task(BaseModel):
    job_id: str = binascii.hexlify(os.urandom(32)).decode("utf-8")
    file_name: Optional[str] = None
    map_function: Union[bytes, str]
    reduce_function: Union[bytes, str]


class WebSocketMessage(BaseModel):
    event: str
    body: dict


""" specifically meant for worker services """


class WorkerTask(Task):
    node_id: int


class FileModel(BaseModel):
    chunk: Union[bytes, str]
    chunk_index: int
    completed: Optional[bool]


class ReduceKeyWithIP(BaseModel):
    key_list: List[str]
    other_worker_node_ip: List[str]


def serialize_file_model(file: FileModel) -> dict:
    file.chunk = base64.b64encode(file.chunk).decode("ascii")
    return file.dict()


def deserialize_file_model(file: dict) -> FileModel:
    file["chunk"] = base64.b64decode(file["chunk"].encode("ascii"))
    return FileModel.parse_obj(file)


def serialize_task(task: Task) -> dict:
    task.map_function = base64.b64encode(task.map_function).decode("ascii")
    task.reduce_function = base64.b64encode(task.reduce_function).decode("ascii")
    return task.dict()


def deserialize_task(task: dict, task_model=Task) -> Union[Task, WorkerTask]:
    task["map_function"] = base64.b64decode(task["map_function"].encode("ascii"))
    task["reduce_function"] = base64.b64decode(task["reduce_function"].encode("ascii"))
    return task_model.parse_obj(task)
