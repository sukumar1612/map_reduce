from services.master_services.master_network_interface import \
    MasterAPIInterface
from services.router_class import Router

app = Router()


@app.add_route(event="add_worker_node")
async def add_worker_node_to_list(websocket, message_body: dict):
    MasterAPIInterface.add_new_worker_node(message_body["ip"])
    await websocket.send("ACK")


@app.add_route(event="initialization_sequence")
async def initialize_app(websocket, message_body: dict):
    await MasterAPIInterface.initialization_sequence()
