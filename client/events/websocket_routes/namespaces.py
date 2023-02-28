import pprint

import socketio


class ClientNamespaceResponseHandlers(socketio.AsyncClientNamespace):
    async def on_result(self, message_body: dict):
        pprint.pprint(message_body)

    async def on_all_file_init_done(self, message_body: dict):
        pprint.pprint(message_body)
        print("file init done")
        await self.disconnect()
