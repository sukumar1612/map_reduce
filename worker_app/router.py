import json

from services.models import (ReduceKeyWithIP, WorkerTask,
                             deserialize_file_model, deserialize_task)
from services.worker_services.router_class import Router
from services.worker_services.worker_network_interface import WorkerInterface

app = Router()


@app.add_route(event="file_stream")
async def build_csv_file_from_chunks_handler(websocket, message_body: dict):
    deserialized_message_body = deserialize_file_model(message_body)
    if deserialized_message_body.completed is True:
        WorkerInterface.CSV_FILE.seek(0)
        await websocket.send("ACK")
        WorkerInterface.LOG.info(f"app : file transfer done")
        return
    WorkerInterface.build_csv_file_from_chunks(chunk=deserialized_message_body.chunk)
    await websocket.send("ACK")


@app.add_route(event="add_new_task")
async def add_new_task_handler(websocket, message_body: dict):
    deserialized_message_body = deserialize_task(message_body, WorkerTask)
    WorkerInterface.add_new_task(deserialized_message_body)
    await websocket.send("ACK")


@app.add_route(event="distinct_keys")
async def get_distinct_keys_from_worker_to_master(websocket, message_body: dict):
    distinct_keys = WorkerInterface.perform_mapping_and_return_distinct_keys()
    await websocket.send(json.dumps({"distinct_keys": distinct_keys}))


@app.add_route(event="assign_reduce_keys_and_shuffle")
async def assign_worker_node_with_reduce_keys(websocket, message_body: dict):
    deserialized_message_body = ReduceKeyWithIP.parse_obj(message_body)
    WorkerInterface.assign_reduce_keys_to_node(message=deserialized_message_body)
    await WorkerInterface.shuffle()
    await websocket.send(
        json.dumps(WorkerInterface.perform_reduce_and_get_final_results())
    )
    WorkerInterface.clear_all_prev_class_values()


@app.add_route(event="get_value_for_keys")
async def get_value_for_keys(websocket, message_body: dict):
    """Not for use by master"""
    result = WorkerInterface.get_value_for_keys(key_list=message_body["key_list"])
    await websocket.send(json.dumps(result))
