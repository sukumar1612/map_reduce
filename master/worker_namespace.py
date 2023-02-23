import asyncio
import pprint

import socketio

from services.master.master_api_interface import MasterAPIInterface

LOCK = asyncio.Lock()


class WorkerNamespace(socketio.AsyncNamespace):
    COUNT_OF_INITIALIZED_NODES: int = 0
    COUNT_OF_MAP_RESULTS_RECEIVED: int = 0
    COUNT_OF_REDUCE_RESULTS_RECEIVED: int = 0

    async def on_connect(self, sid: str, environ: dict):
        print(f'http://{environ.get("asgi.scope").get("client")[0]}:7000')
        # print(f"------{await self.server.session()}------")
        # pprint.pprint(self.session(sid=sid, namespace='/worker'))
        node_id = MasterAPIInterface.add_new_node(
            node_sid=sid,
            node_ip=f'http://{environ.get("asgi.scope").get("client")[0]}:7000',
        )
        print(f"node with sid {sid} requested connection")
        await self.emit(
            "assign_node_id", {"node_id": node_id}, room=sid, namespace="/worker"
        )

    async def on_worker_node_initialization_done(self, sid: str, message_body: dict):
        print(
            f"""
        File sent to {list(filter(lambda x: x[1][0] == sid, MasterAPIInterface.CONNECTED_NODES_METADATA.items()))[0]}
        """
        )
        self.COUNT_OF_INITIALIZED_NODES += 1
        print(f"___number of files sent: {self.COUNT_OF_INITIALIZED_NODES}___")

    async def on_get_map_results(self, sid, message_body: dict):
        self.COUNT_OF_MAP_RESULTS_RECEIVED += 1
        async with LOCK:
            MasterAPIInterface.insert_map_result_data(map_keys=message_body["map_keys"])
        print(f"___number of map result sent: {self.COUNT_OF_MAP_RESULTS_RECEIVED}___")
        print(f"map results: {message_body['map_keys']}")
        if (
            self.COUNT_OF_MAP_RESULTS_RECEIVED
            == MasterAPIInterface.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK
        ):
            self.COUNT_OF_REDUCE_RESULTS_RECEIVED = (
                await MasterAPIInterface.assign_reduce_keys(socket_connection=self)
            )

    async def on_get_final_result(self, sid, message_body: dict):
        print(message_body)
        MasterAPIInterface.insert_partial_result(message_body)
        await self.emit("prepare_for_next_task", {}, room=sid, namespace="/worker")
        self.COUNT_OF_REDUCE_RESULTS_RECEIVED -= 1
        if self.COUNT_OF_REDUCE_RESULTS_RECEIVED <= 0:
            MasterAPIInterface.RESULTS[
                MasterAPIInterface.CURRENT_TASK.job_id
            ] = MasterAPIInterface.MASTER_NODE_HANDLER.get_final_reduced_data()
            MasterAPIInterface.prepare_for_next_task()
            self.COUNT_OF_INITIALIZED_NODES = 0
            self.COUNT_OF_MAP_RESULTS_RECEIVED = 0
