import socketio

from services.worker.worker_api_interface import WorkerAPIInterface

p2p_server = socketio.Server()
app = socketio.WSGIApp(
    p2p_server,
    static_files={"/": {"content_type": "text/html", "filename": "index.html"}},
)


class P2PServerNamespace(socketio.Namespace):
    def on_request_value_for_list_of_keys(self, sid: str, message_body: dict):
        print(f' received key list : {message_body["key_list"]}')
        self.emit(
            "receive_key_value",
            WorkerAPIInterface.fetch_mapped_value_for_specific_key(
                message_body["key_list"]
            ),
            room=sid,
            namespace="/p2p",
        )


p2p_server.register_namespace(P2PServerNamespace("/p2p"))
