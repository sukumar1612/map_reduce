from typing import List

from common.models import Task


class StateManager:
    jobs: List[Task] = []
