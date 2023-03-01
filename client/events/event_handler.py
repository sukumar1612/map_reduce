from client.events.http_routes.routes import http_handler
from client.events.terminal_commands.routes import commands_handler
from client.events.websocket_routes.namespaces import \
    ClientNamespaceResponseHandlers
from client.events.websocket_routes.routes import websocket_handler
from client.services.router import Router

app = Router(ip="localhost", port=5000)
app.add_from_blue_print(http_handler)
app.add_from_blue_print(websocket_handler)
app.add_from_blue_print(commands_handler)
app.sio.register_namespace(ClientNamespaceResponseHandlers("/client"))
