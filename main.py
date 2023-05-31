from fastapi import FastAPI
from typing import List
from pydantic import BaseModel

from service import generate_report

app = FastAPI()


class Report(BaseModel):
    store_id: str
    uptime_last_hour: int
    uptime_last_day: int
    uptime_last_week: int
    downtime_last_hour: int
    downtime_last_day: int
    downtime_last_week: int


@app.get("/trigger_report", response_model=str)
def trigger_report(csv_files: List[str]):
    # Call the generate_report function with the provided CSV files
    report_data = generate_report(csv_files)

    # Save the report data and return the report_id
    # Here, you can implement your logic to store the report data in the database and generate a unique report_id
    report_id = "xyz123"

    return report_id


@app.get("/get_report", response_model=Report)
def get_report(report_id: str):
    # Here, you can implement your logic to retrieve the report data based on the report_id from the database
    # Return the report data or appropriate status based on its availability
    if report_id == "xyz123":
        # Example report data
        return Report(**report_data[0])
    else:
        return "Report not found"
