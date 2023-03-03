import copy
import json

import requests

from client.services.router import BluePrint, EventTypes
from client.services.state_manager import StateManager
from common.models import serialize_task

http_handler = BluePrint()


@http_handler.add_route(event="fetch_results", event_type=EventTypes.HTTP)
def fetch_results(http_base_url: str):
    """fetch results submitted to master node"""
    for job in StateManager.jobs:
        print(f"id: {job.job_id}")
        response = requests.get(
            url=f"{http_base_url}/fetch-result", params={"job_id": job.job_id}
        ).json()
        print(f"Result: {response}")


@http_handler.add_route(event="fetch_all_results", event_type=EventTypes.HTTP)
def fetch_all_results(http_base_url: str):
    """fetch all results submitted to master node"""
    response = requests.get(url=f"{http_base_url}/fetch-all-results").json()
    print(f"Result: {response}")


@http_handler.add_route(event="add_tasks", event_type=EventTypes.HTTP)
def add_tasks(http_base_url: str):
    """add a task to master node"""
    for job in StateManager.jobs:
        serialized_task = json.dumps(serialize_task(copy.deepcopy(job)))
        requests.post(url=f"{http_base_url}/add-task", json={"task": serialized_task})

    print(f"Result: added {len(StateManager.jobs)} tasks")


@http_handler.add_route(event="delete_all_results", event_type=EventTypes.HTTP)
def delete_all_results(http_base_url: str):
    """delete all results stored in master node"""
    requests.delete(url=f"{http_base_url}/delete-all-results")

    print("Result: cleared all results from master node cache")
