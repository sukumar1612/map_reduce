import sys

from master_app.index import main_app_master
from worker_app.index import main_app_worker

if __name__ == "__main__":
    if sys.argv[1] == "-master":
        main_app_master.run_app("localhost", port=int(sys.argv[2]))
    else:
        main_app_worker.run_app("localhost", port=int(sys.argv[2]))
