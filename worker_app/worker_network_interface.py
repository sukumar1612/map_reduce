import tempfile

from services.master_worker_service.worker_class import worker_factory
from services.models import WorkerTask


class WorkerInterface:
    CURRENT_TASK = None
    CSV_FILE = None
    CURRENT_JOB = None

    @classmethod
    def add_new_task(cls, task: WorkerTask) -> None:
        cls.CURRENT_TASK = task

    @classmethod
    def build_csv_file_from_chunks(cls, chunk: bytes):
        if cls.CSV_FILE is None:
            cls.CSV_FILE = tempfile.NamedTemporaryFile()
            cls.CURRENT_TASK.file_name = cls.CSV_FILE.name
        cls.CSV_FILE.write(chunk)

    @classmethod
    def perform_mapping_and_return_distinct_keys(cls) -> list:
        if cls.CURRENT_JOB is None:
            cls.CURRENT_JOB = worker_factory(task=cls.CURRENT_TASK)
        cls.CURRENT_JOB.perform_mapping()
        return cls.CURRENT_JOB.get_list_of_distinct_keys()
