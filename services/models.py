import binascii
import os

from pydantic import BaseModel


class Task(BaseModel):
    job_id: str = binascii.hexlify(os.urandom(32)).decode("utf-8")
    file_name: str
    map_function: bytes
    reduce_function: bytes
