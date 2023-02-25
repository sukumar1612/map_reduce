import base64
import binascii
import os
from typing import Optional, Union

from pydantic import BaseModel, root_validator, validator


class Task(BaseModel):
    job_id: Optional[str] = None
    file_name: Optional[str] = None
    mapper_function: Union[bytes, str]
    reducer_function: Union[bytes, str]

    @validator("job_id", pre=True, always=True)
    def assign_job_id(cls, job_id):
        if job_id is None:
            return binascii.hexlify(os.urandom(32)).decode("utf-8")
        return job_id


class FileModel(BaseModel):
    chunk: Union[bytes, str]
    chunk_index: int
    completed: Optional[bool]


def serialize_file_model(file: FileModel) -> dict:
    file.chunk = base64.b64encode(file.chunk).decode("ascii")
    return file.dict()


def deserialize_file_model(file: dict) -> FileModel:
    file["chunk"] = base64.b64decode(file["chunk"].encode("ascii"))
    return FileModel.parse_obj(file)


def serialize_task(task: Task) -> dict:
    task.mapper_function = base64.b64encode(task.mapper_function).decode("ascii")
    task.reducer_function = base64.b64encode(task.reducer_function).decode("ascii")
    return task.dict()


def deserialize_task(task: dict) -> Task:
    task["mapper_function"] = base64.b64decode(task["mapper_function"].encode("ascii"))
    task["reducer_function"] = base64.b64decode(
        task["reducer_function"].encode("ascii")
    )
    return Task.parse_obj(task)
