import sys

from worker.router import worker_process

if __name__ == "__main__":
    # worker_process(host='localhost')
    worker_process(host=sys.argv[1])
