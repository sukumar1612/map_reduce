from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from master.services.master_api_interface import MasterAPIInterface

app = FastAPI()


class TaskMessage(BaseModel):
    task: str


@app.post("/add-task")
async def add_task(task: TaskMessage):
    await MasterAPIInterface.add_task(task.task)
    return Response(status_code=201)


@app.delete("/delete-all-results")
async def delete_all_results():
    MasterAPIInterface.RESULTS.clear()


@app.get("/fetch-result")
async def fetch_result(job_id: str):
    if (
        job_id in MasterAPIInterface.RESULTS.keys()
        and MasterAPIInterface.RESULTS[job_id].get("result", None) is not None
    ):
        return JSONResponse(
            content=MasterAPIInterface.fetch_result(job_id), status_code=200
        )
    elif job_id in MasterAPIInterface.RESULTS.keys():
        return JSONResponse(content={"status": "In Progress"}, status_code=200)
    else:
        return JSONResponse(content={"status": "Queued"}, status_code=200)
