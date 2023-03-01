import asyncio

import socketio

from master.services.master_api_interface import MasterAPIInterface

LOCK = asyncio.Lock()


class WorkerNamespace(socketio.AsyncNamespace):
    async def on_connect(self, sid: str, environ: dict):
        print(f'http://{environ.get("asgi.scope").get("client")[0]}:7000')
        node_id = MasterAPIInterface.add_new_node(
            node_sid=sid,
            node_ip=f'http://{environ.get("asgi.scope").get("client")[0]}:7000',
        )
        print(f"node with sid {sid} requested connection")
        await self.emit(
            "assign_node_id", {"node_id": node_id}, room=sid, namespace="/worker"
        )

    async def on_file_init_done(self, sid: str, message_body: dict):
        MasterAPIInterface.COUNT_OF_INITIALIZED_NODES += 1
        print(
            f"___number of files sent: {MasterAPIInterface.COUNT_OF_INITIALIZED_NODES}___"
        )
        if (
            MasterAPIInterface.COUNT_OF_INITIALIZED_NODES
            == MasterAPIInterface.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK
        ):
            await self.emit(event="all_file_init_done", data={}, namespace="/client")

    async def on_get_map_results(self, sid, message_body: dict):
        async with LOCK:
            MasterAPIInterface.insert_map_result_data(map_keys=message_body["map_keys"])
        print(
            f"___number of map result sent: {MasterAPIInterface.COUNT_OF_MAP_RESULTS_RECEIVED}___"
        )
        print(f"map results: {message_body['map_keys']}")
        if (
            MasterAPIInterface.COUNT_OF_MAP_RESULTS_RECEIVED
            == MasterAPIInterface.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK
        ):
            await MasterAPIInterface.assign_reduce_keys(socket_connection=self)

    async def on_get_final_result(self, sid, message_body: dict):
        print(message_body)
        MasterAPIInterface.insert_partial_result(message_body)
        await self.emit("prepare_for_next_task", {}, room=sid, namespace="/worker")

        if MasterAPIInterface.COUNT_OF_REDUCE_RESULTS_RECEIVED <= 0:
            MasterAPIInterface.prepare_for_next_task()
