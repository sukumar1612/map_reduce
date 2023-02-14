import json

from services.models import (ReduceKeyWithIP, WorkerTask,
                             deserialize_file_model, deserialize_task)
import socketio
from services.worker_services.worker_network_interface import \
    WorkerAPIInterface

sio_worker = socketio.Client()


@sio_worker.event(namespace='/worker')
def connect():
    print('______connecting to server______')


@sio_worker.event(namespace='/worker')
def on_connect(message_body: dict):
    WorkerAPIInterface.set_worker_node_id(message_body['node_id'])
    print(f'connected to master, worker node id : {WorkerAPIInterface.get_worker_node_id()}')


@sio_worker.on('file_initialization', namespace='/worker')
def build_csv_file_from_chunks_handler(message_body: dict):
    deserialized_message_body = deserialize_file_model(json.loads(message_body['file']))
    print(f"received chunk {deserialized_message_body.chunk_index + 1}")
    if deserialized_message_body.completed is True:
        WorkerAPIInterface.CSV_FILE.seek(0)
        sio_worker.emit(
            "file_initialization_done",
            {"message": "acknowledge"},
            namespace='/worker'
        )
        return
    WorkerAPIInterface.build_csv_file_from_chunks(chunk=deserialized_message_body.chunk)


@sio_worker.on("perform_map_reduce", namespace='/worker')
def get_task_and_perform_map_reduce(message_body: dict):
    print(message_body)
    deserialized_message_body = deserialize_task(json.loads(message_body['task']), WorkerTask)
    WorkerAPIInterface.add_new_task(deserialized_message_body)
    distinct_keys = WorkerAPIInterface.perform_mapping_and_return_distinct_keys()
    print(distinct_keys)
    sio_worker.emit(
        "map_results",
        {
            "distinct_keys": distinct_keys
        },
        namespace='/worker'
    )


if __name__ == '__main__':
    sio_worker.connect('http://localhost:5000', namespaces=['/worker'])
    sio_worker.wait()
# @app.on("add_new_task")
# async def add_new_task_handler(websocket, message_body: dict):
#     deserialized_message_body = deserialize_task(message_body, WorkerTask)
#     WorkerAPIInterface.add_new_task(deserialized_message_body)
#     await websocket.send("ACK")
#
#
# @app.on("distinct_keys")
# async def get_distinct_keys_from_worker_to_master(websocket, message_body: dict):
#     distinct_keys = WorkerAPIInterface.perform_mapping_and_return_distinct_keys()
#     await websocket.send(json.dumps({"distinct_keys": distinct_keys}))
#
#
# @app.on("assign_reduce_keys_and_shuffle")
# async def assign_worker_node_with_reduce_keys(websocket, message_body: dict):
#     deserialized_message_body = ReduceKeyWithIP.parse_obj(message_body)
#     WorkerAPIInterface.assign_reduce_keys_to_node(message=deserialized_message_body)
#     await WorkerAPIInterface.shuffle()
#     await websocket.send(
#         json.dumps(WorkerAPIInterface.perform_reduce_and_get_final_results())
#     )
#     WorkerAPIInterface.clear_all_prev_class_values()
#
#
# @app.on("get_value_for_keys")
# async def get_value_for_keys(websocket, message_body: dict):
#     """Not for use by master"""
#     result = WorkerAPIInterface.get_value_for_keys(key_list=message_body["key_list"])
#     await websocket.send(json.dumps(result))
