import json

import requests

from client.services.router import BluePrint
from client.services.state_manager import StateManager
from common.models import serialize_task

http_handler = BluePrint()


@http_handler.add_http_route(event="fetch_results")
def fetch_results(http_base_url: str):
    for job in StateManager.jobs:
        response = requests.get(
            url=f"{http_base_url}/fetch-result", params={"job_id": job.job_id}
        ).json()
        print(f"Result: {response}")


@http_handler.add_http_route(event="fetch_all_results")
def fetch_all_results(http_base_url: str):
    response = requests.get(url=f"{http_base_url}/fetch-all-results").json()
    print(f"Result: {response}")


@http_handler.add_http_route(event="add_tasks")
def add_tasks(http_base_url: str):
    for job in StateManager.jobs:
        serialized_task = json.dumps(serialize_task(job))
        requests.post(url=f"{http_base_url}/add-task", json={"task": serialized_task})

    print(f"Result: added {len(StateManager.jobs)} tasks")


@http_handler.add_http_route(event="delete_all_results")
def delete_all_results(http_base_url: str):
    requests.delete(url=f"{http_base_url}/delete-all-results")

    print("Result: cleared all results from master node cache")
