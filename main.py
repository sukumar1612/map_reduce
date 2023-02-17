import sys

import eventlet

from master.router import app
from worker.router import worker_process

if __name__ == "__main__":
    # worker_process(host='localhost')
    if sys.argv[1] == "-master":
        eventlet.wsgi.server(eventlet.listen(("", 5000)), app)
    elif sys.argv[1] == "-worker":
        worker_process(host=sys.argv[2])
