import asyncio

import socketio

from common.logger_module import get_logger
from master.services.master_api_interface import JobTracker

LOCK = asyncio.Lock()
LOG = get_logger(__name__)


class WorkerNamespace(socketio.AsyncNamespace):
    async def on_connect(self, sid: str, environ: dict):
        node_id = JobTracker.add_new_node(
            node_sid=sid,
            node_ip=f'http://{environ.get("asgi.scope").get("client")[0]}:7000',
        )
        await self.emit(
            "assign_node_id", {"node_id": node_id}, room=sid, namespace="/worker"
        )
        LOG.debug(f'http://{environ.get("asgi.scope").get("client")[0]}:7000 connected')

    async def on_file_init_done(self, sid: str, message_body: dict):
        JobTracker.COUNT_OF_INITIALIZED_NODES += 1
        LOG.debug(
            f"number of worker nodes initialized : {JobTracker.COUNT_OF_INITIALIZED_NODES}"
        )
        if (
            JobTracker.COUNT_OF_INITIALIZED_NODES
            == JobTracker.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK
        ):
            await self.emit(event="all_file_init_done", data={}, namespace="/client")

    async def on_get_map_results(self, sid, message_body: dict):
        async with LOCK:
            JobTracker.insert_map_result_data(map_keys=message_body["map_keys"])
        LOG.debug(
            f"number of worker nodes initialized : {JobTracker.COUNT_OF_MAP_RESULTS_RECEIVED}"
        )
        LOG.debug(f"map results: {message_body['map_keys']}")
        if (
            JobTracker.COUNT_OF_MAP_RESULTS_RECEIVED
            == JobTracker.NUMBER_OF_NODES_CURRENTLY_USED_IN_TASK
        ):
            LOG.debug(f"all map results have been received")
            await JobTracker.assign_reduce_keys(socket_connection=self)

    async def on_get_final_result(self, sid, message_body: dict):
        LOG.debug(f"message received: {message_body}")
        JobTracker.insert_partial_result(message_body)
        await self.emit("prepare_for_next_task", {}, room=sid, namespace="/worker")

        if JobTracker.COUNT_OF_REDUCE_RESULTS_RECEIVED <= 0:
            LOG.debug(
                f"final result: {JobTracker.RESULTS[JobTracker.CURRENT_TASK.job_id]}"
            )
            JobTracker.prepare_for_next_task()
