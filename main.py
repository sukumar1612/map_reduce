import sys

import eventlet

from master_app.router import app
from worker_app.router import sio_worker

if __name__ == "__main__":
    if sys.argv[1] == "-master":
        eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
    else:
        sio_worker.connect('http://localhost:5000')
        sio_worker.wait()
