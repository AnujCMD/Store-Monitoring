import uuid
from typing import Dict

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.background import BackgroundTasks
from starlette.responses import JSONResponse

from service import generate_report, update_report, fetch_report

app = FastAPI()

generated_report_response = {}


class Report(BaseModel):
    data: Dict[str, str]
    status: str


@app.get("/trigger_report", response_model=str)
def trigger_report(background_tasks: BackgroundTasks):
    report_id = uuid.uuid4().hex
    generated_report_response[report_id] = {"status": "running", "result": None}
    background_tasks.add_task(generate_report, report_id)
    return report_id


@app.get("/update_timestamp", response_model=str)
def update_timestamp():
    update_report()
    return ''


@app.get("/get_report", response_model=Report)
def get_report(report_id: str):
    if report_id in generated_report_response:
        return generated_report_response['report_id']
    else:
        return {"error": "Invalid report_id"}


@app.get("/hello")
def test_run():
    return "Hello_Fast_API"
