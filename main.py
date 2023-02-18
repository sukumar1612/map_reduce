import sys

import eventlet

from master.router import app
from worker.router import worker_process

if __name__ == "__main__":
    if sys.argv[1] == "-master":
        eventlet.wsgi.server(eventlet.listen((sys.argv[2], 5000)), app)
    elif sys.argv[1] == "-worker":
        worker_process(p2p_server_host=sys.argv[2], master_server_host=sys.argv[3])

    # todo
    # 1. add data streaming between p2p servers -> stream objects
    # 2. make server asynchronous and make p2p server asynchronous (better performance for IO ops)
    # 3. add client side server with fastapi + messaging queue for handling tasks ?
    # 4. make client with typer or something similar
    # 5. manage dependencies with poetry
    # 6. switch to python 3.10
