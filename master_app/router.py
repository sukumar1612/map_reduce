import socketio
import eventlet

from services.master_services.master_network_interface import \
    MasterAPIInterface

sio_master = socketio.Server()
app = socketio.WSGIApp(sio_master, static_files={
    '/': {'content_type': 'text/html', 'filename': 'index.html'}
})

prev_sid = None


@sio_master.on('connect', namespace='/client')
def connect_client(sid, message_body: dict):
    print("_________client connected__________")


@sio_master.on('connect', namespace='/worker')
def add_worker_node_to_list(sid, message_body: dict):
    global prev_sid
    prev_sid = sid
    _id = MasterAPIInterface.add_new_worker_node(sid)
    print(f'connected to : {sid} with ID : {_id}')
    sio_master.emit(
        'on_connect',
        {'node_id': _id},
        room=sid,
        namespace='/worker'
    )


@sio_master.on("file_initialization", namespace='/client')
def initialize_app(sid, message_body: dict):
    MasterAPIInterface.initialization_sequence(sio_master)


@sio_master.on("file_initialization_done", namespace='/worker')
def show_initialized_app(sid, message_body: dict):
    print("FILE sent")
    print(message_body)


@sio_master.on("perform_map_reduce", namespace='/client')
def distribute_task_and_perform_map_reduce(sid, message_body: dict):
    MasterAPIInterface.distribute_task_and_perform_map_reduce(sio_master)


@sio_master.on('map_results', namespace='/worker')
def get_map_reduce_results(sid, message_body: dict):
    print(message_body['distinct_keys'])
    MasterAPIInterface.insert_map_result_data(message_body['distinct_keys'])


@sio_master.on('environment', namespace='/client')
def print_all_ip_addresses_of_nodes(sid, message_body: dict):
    global prev_sid
    print(prev_sid)
    environment = sio_master.get_environ(prev_sid, namespace='/worker')
    print(environment['REMOTE_ADDR'])
    print(environment['REMOTE_PORT'])


if __name__ == '__main__':
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
