import base64
import pickle
import tempfile

import socketio

from common.logger_module import get_logger
from worker.services.worker_api_interface import TaskTracker

p2p_server = socketio.Server()
app = socketio.WSGIApp(p2p_server)
LOG = get_logger(__name__)
CHUNK_SIZE = 1024 * 100


class P2PServerNamespace(socketio.Namespace):
    def on_request_value_for_list_of_keys(self, sid: str, message_body: dict):
        LOG.debug(f' received key list : {message_body["key_list"]}')
        file = tempfile.NamedTemporaryFile()
        pickle.dump(
            TaskTracker.fetch_mapped_value_for_specific_key(message_body["key_list"]),
            file,
            pickle.HIGHEST_PROTOCOL,
        )
        file.seek(0)
        chunk = file.read(CHUNK_SIZE)
        while chunk:
            self.emit(
                "receive_key_value",
                {"chunk": base64.b64encode(chunk).decode("ascii"), "completed": False},
                room=sid,
                namespace="/p2p",
            )
            chunk = file.read(CHUNK_SIZE)
        self.emit(
            "receive_key_value",
            {"chunk": "", "completed": True},
            room=sid,
            namespace="/p2p",
        )


p2p_server.register_namespace(P2PServerNamespace("/p2p"))
