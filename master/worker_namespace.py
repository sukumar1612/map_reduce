import socketio

from services.master.master_api_interface import MasterAPIInterface


class WorkerNamespace(socketio.Namespace):
    COUNT_OF_INITIALIZED_NODES: int = 0
    COUNT_OF_MAP_RESULTS_RECEIVED: int = 0

    def on_connect(self, sid: str, environ: dict):
        node_id = MasterAPIInterface.add_new_node(
            node_sid=sid, node_ip=f'http://{environ.get("REMOTE_ADDR")}:7000'
        )
        print(f"node with sid {sid} requested connection")
        self.emit("assign_node_id", {"node_id": node_id}, room=sid, namespace="/worker")

    def on_worker_node_initialization_done(self, sid: str, message_body: dict):
        print(
            f"""
        File sent to {list(filter(lambda x: x[1][0] == sid, MasterAPIInterface.CONNECTED_NODES_METADATA.items()))[0]}
        """
        )
        self.COUNT_OF_INITIALIZED_NODES += 1
        print(f"___number of files sent: {self.COUNT_OF_INITIALIZED_NODES}___")

    def on_get_map_results(self, sid, message_body: dict):
        self.COUNT_OF_MAP_RESULTS_RECEIVED += 1
        MasterAPIInterface.insert_map_result_data(map_keys=message_body["map_keys"])
        print(f"___number of map result sent: {self.COUNT_OF_MAP_RESULTS_RECEIVED}___")
        print(f"map results: {message_body['map_keys']}")

    def on_get_final_result(self, sid, message_body: dict):
        print(message_body)
