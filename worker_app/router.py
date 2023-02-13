import json

from services.models import (WorkerTask, deserialize_file_model,
                             deserialize_task)
from worker_app.router_class import Router
from worker_app.worker_network_interface import WorkerInterface

app = Router()


@app.add_route(event="add_new_task")
async def add_new_task_handler(websocket, message_body: dict):
    deserialized_message_body = deserialize_task(message_body, WorkerTask)
    WorkerInterface.add_new_task(deserialized_message_body)
    await websocket.send("ACK")


@app.add_route(event="file_stream")
async def build_csv_file_from_chunks_handler(websocket, message_body: dict):
    deserialized_message_body = deserialize_file_model(message_body)
    if deserialized_message_body.completed is True:
        WorkerInterface.CSV_FILE.seek(0)
        print(WorkerInterface.CSV_FILE.name)
        await websocket.send("ACK")
        return
    WorkerInterface.build_csv_file_from_chunks(chunk=deserialized_message_body.chunk)
    await websocket.send("ACK")


@app.add_route(event="distinct_keys")
async def get_distinct_keys_from_worker_to_master(websocket, _message_body: dict):
    distinct_keys = WorkerInterface.perform_mapping_and_return_distinct_keys()
    await websocket.send(json.dumps({"distinct_keys": distinct_keys}))


@app.add_route(event="assign_reduce_keys")
async def assign_worker_node_with_reduce_keys(websocket, _message_body: dict):
    pass
