import os
import sys

import dotenv
import uvicorn

from master.router import fastapi_app
from worker.router import worker_process

dotenv.load_dotenv()

if __name__ == "__main__":
    if sys.argv[1] == "-master":
        uvicorn.run(fastapi_app, host="0.0.0.0", port=5000)
        # eventlet.wsgi.server(eventlet.listen((os.getenv("MASTER_HOST_IP"), 5000)), app)
    elif sys.argv[1] == "-worker":
        worker_process(
            p2p_server_host=os.getenv("WORKER_HOST_IP"),
            master_server_host=os.getenv("MASTER_HOST_IP"),
        )

    # todo
    # 1. add simple jwt authentication to the fastapi + socketio server
    # 2. make client with typer or something similar -> priority 3
    # 3. create docker file and orchestrate with kubernetes -> priority -> 1
