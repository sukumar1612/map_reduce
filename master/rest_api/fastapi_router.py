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


@app.get("/fetch-result")
async def fetch_result(job_id: str):
    return JSONResponse(
        content=MasterAPIInterface.fetch_result(job_id), status_code=200
    )
