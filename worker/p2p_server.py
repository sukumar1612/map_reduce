import base64
import pickle
import tempfile

import socketio

from services.worker.worker_api_interface import WorkerAPIInterface

p2p_server = socketio.Server()
app = socketio.WSGIApp(
    p2p_server,
    static_files={"/": {"content_type": "text/html", "filename": "index.html"}},
)

CHUNK_SIZE = 1024 * 100


class P2PServerNamespace(socketio.Namespace):
    def on_request_value_for_list_of_keys(self, sid: str, message_body: dict):
        print(f' received key list : {message_body["key_list"]}')
        file = tempfile.NamedTemporaryFile()
        pickle.dump(
            WorkerAPIInterface.fetch_mapped_value_for_specific_key(
                message_body["key_list"]
            ),
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
