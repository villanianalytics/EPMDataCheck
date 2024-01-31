import json

from starlette.responses import PlainTextResponse

from app.core.epm_functions import *
from fastapi import APIRouter, HTTPException, Body, UploadFile, File, Form, requests

router = APIRouter()


@router.post("/export_data_slice_json/")
async def epm_export_data_slice_json(
        base_url: str = Form(...),
        username: str = Form(...),
        password: str = Form(...),
        app_name: str = Form(...),
        api_version: str = Form(...),
        plan_type_name: str = Form(...),
        payload: str = Form(...)  # Accepting payload as a string
):
    try:
        logging.info("Received request to export data from epm")
        export_data_slice_json(base_url, username, password, app_name, api_version, plan_type_name, payload)
    except Exception as e:
        logging.error(f"Error in export data: {e}")


@router.post("/import_data_slice_json/")
async def epm_import_data_slice_json(
        base_url: str = Form(...),
        username: str = Form(...),
        password: str = Form(...),
        app_name: str = Form(...),
        api_version: str = Form(...),
        plan_type_name: str = Form(...),
        payload: str = Form(...)  # Accepting payload as a string or a file
):
    try:
        logging.info("Received request to import data to epm")
        import_data_slice_json (base_url, username, password, app_name, api_version, plan_type_name, payload)
    except Exception as e:
        logging.error(f"Error in import data: {e}")


@router.post("/run_job/")
async def run_job(
        background_tasks: BackgroundTasks,
        base_url: str = Form(...),
        api_version: str = Form(...),
        application: str = Form(...),
        job_type: str = Form(...),
        job_name: str = Form(...),
        username: str = Form(...),
        password: str = Form(...),
        parameters: str = Form(None),  # Accepting parameters as a JSON string
        poll_interval: int = Form(10),  # Default poll interval in seconds
        max_retries: int = Form(30)  # Default maximum number of retries
):
    try:
        logging.info("Received request to run a job")
        run_job(background_tasks, base_url, api_version, application, job_type, job_name, username, password, parameters, poll_interval, max_retries)
    except Exception as e:
        logging.error(f"Error in run job: {e}")

