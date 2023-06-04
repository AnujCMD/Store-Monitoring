import uuid

from fastapi import FastAPI
from starlette.background import BackgroundTasks

from service.service_impl import generate_report, update_report, generated_report_response

app = FastAPI(
    title='Store Monitoring',
    description='Gives information about whether the store is online or not during the business hours'
)


# API to trigger report generation, returns report_id immediately and triggers report generation as Background Task
@app.get("/trigger_report", response_model=dict)
def trigger_report(background_tasks: BackgroundTasks):
    # Generate the UUID for unique report_id for every request
    report_id = uuid.uuid4().hex
    generated_report_response[report_id] = {"status": "running"}
    # Add the report generation into background task and return the report id immediately
    background_tasks.add_task(generate_report, report_id)
    return {'report_id': report_id}


# API to update the timestamp from string to DateTime object in Mongo
@app.get("/update_timestamp", response_model=str)
def update_timestamp():
    update_report()
    return ''


# API to get the report, takes report_id as Request Param
@app.get("/get_report", response_model=dict)
def get_report(report_id: str):
    if report_id in generated_report_response:
        return generated_report_response.get(report_id)
    else:
        return {"error": "Invalid report_id"}


# Test API for checking the status of server
@app.get("/hello")
def test_run():
    return "Hello_Fast_API"
