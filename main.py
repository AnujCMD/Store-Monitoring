import uuid
from typing import List, Dict

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.responses import JSONResponse

from config.mongo_connection import connect_mongo
from service import generate_report, update_report, fetch_report

app = FastAPI()


class Report(BaseModel):
    data: Dict[str, str]
    status: str


@app.get("/trigger_report", response_model=str)
def trigger_report():
    report_id = uuid.uuid4().hex
    generate_report(report_id)
    return report_id


@app.get("/update_timestamp", response_model=str)
def update_timestamp():
    update_report()
    return ''


@app.get("/get_report", response_model=Report)
def get_report(report_id: str):
    data = fetch_report(report_id)
    if data is not None:
        return Report(
            data=data,
            status="Success"
        )
    else:
        return JSONResponse(content={"status": "Pending"}, status_code=200)


@app.get("/hello")
def test_run():
    return "Hello_Fast_API"
