from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from common.logger_module import get_logger
from master.services.master_api_interface import JobTracker

app = FastAPI()

LOG = get_logger(__name__)


class TaskMessage(BaseModel):
    task: str


@app.post("/add-task")
async def add_task(task: TaskMessage) -> Response:
    await JobTracker.add_task(task.task)
    LOG.debug(f"added task {task.task}")
    return Response(status_code=201)


@app.delete("/delete-all-results")
async def delete_all_results() -> Response:
    JobTracker.RESULTS.clear()
    LOG.debug(f"All results are deleted")
    return Response(status_code=204)


@app.get("/fetch-result")
async def fetch_result(job_id: str) -> Response:
    LOG.debug(f"Result: {JobTracker.RESULTS.get(job_id)}")
    if (
        job_id in JobTracker.RESULTS.keys()
        and JobTracker.RESULTS[job_id].get("result", None) is not None
    ):
        return JSONResponse(content=JobTracker.fetch_result(job_id), status_code=200)
    elif job_id in JobTracker.RESULTS.keys():
        LOG.debug("task in progress")
        return JSONResponse(content={"status": "In Progress"}, status_code=200)
    else:
        LOG.debug("task queued")
        return JSONResponse(content={"status": "Queued"}, status_code=200)


@app.get("/fetch-all-results")
async def fetch_all_results() -> Response:
    final_results = []
    for job_id, data in JobTracker.RESULTS.items():
        if data.get("result", None) is not None:
            final_results.append(data)
        else:
            final_results.append({"job_id": job_id, "status": "In Progress"})

    LOG.debug(f"All Result: {final_results}")
    return JSONResponse(content=final_results, status_code=200)
