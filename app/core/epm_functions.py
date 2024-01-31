import csv
import io
import json
import time

from fastapi import requests, HTTPException
from starlette.background import BackgroundTasks

from app.core.logging_engine import *


def export_data_slice_json(
        base_url: str,
        username: str,
        password: str,
        app_name: str,
        api_version: str,
        plan_type_name: str,
        payload: str):
    try:
        payload_dict = json.loads(payload)  # Converting JSON string to dict
    except json.JSONDecodeError:
        return {"detail": "Malformed JSON payload"}

    url = f"{base_url}/rest/{api_version}/applications/{app_name}/plantypes/{plan_type_name}/exportdataslice"

    response = requests.post(
        url,
        json=payload_dict,
        auth=(username, password)
    )

    if response.status_code == 200:
        return {"status": "success", "data": response.json()}
    else:
        raise HTTPException(status_code=response.status_code, detail=response.text)


def import_data_slice_json(
        base_url: str,
        username: str,
        password: str,
        app_name: str,
        api_version: str,
        plan_type_name: str,
        payload: str):
    try:
        # Convert JSON string to dict
        payload_dict = json.loads(payload)
    except json.JSONDecodeError:
        return {"detail": "Malformed JSON payload"}

    # Construct the URL for the importdataslice endpoint
    url = f"{base_url}/rest/{api_version}/applications/{app_name}/plantypes/{plan_type_name}/importdataslice"

    # Send the POST request to the importdataslice endpoint
    response = requests.post(
        url,
        json=payload_dict,
        auth=(username, password)
    )

    # Check the response status and return accordingly
    if response.status_code == 200:
        return {"status": "success", "data": response.json()}
    else:
        # Log the error details for troubleshooting
        logging.error(f"Error in /import_data_slice_json/: {response.text}")
        raise HTTPException(status_code=response.status_code, detail=response.text)


def run_job(
        background_tasks: BackgroundTasks,
        base_url: str,
        api_version: str,
        application: str,
        job_type: str,
        job_name: str,
        username: str,
        password: str,
        parameters: str,  # Accepting parameters as a JSON string
        poll_interval: int = 10,  # Default poll interval in seconds
        max_retries: int = 30  # Default maximum number of retries
):
    # Construct the URL for the jobs endpoint
    url = f"{base_url}/rest/{api_version}/applications/{application}/jobs"

    # Prepare the payload
    payload = {
        "jobType": job_type,
        "jobName": job_name,
    }

    # If parameters are provided, convert them from JSON string to a dictionary and add to the payload
    if parameters:
        try:
            parameters_dict = json.loads(parameters)
            payload["parameters"] = parameters_dict
        except json.JSONDecodeError:
            return {"detail": "Malformed JSON in parameters field"}

    # Send the POST request to the jobs endpoint with basic authentication
    job_submission_response = requests.post(
        url,
        json=payload,
        auth=(username, password)
    )

    # Check the submission response status and capture the jobID
    if job_submission_response.status_code == 200:
        job_info = job_submission_response.json()
        job_id = job_info.get("jobId")

        # Schedule the background task to poll the job status
        background_tasks.add_task(
            poll_job_status,
            base_url,
            api_version,
            application,
            job_id,
            username,
            password,
            poll_interval,
            max_retries
        )
        return {"message": "Job submitted successfully", "jobId": job_id}
    else:
        # Log the error details for troubleshooting
        logging.error(f"Error in job submission /run_job/: {job_submission_response.text}")
        raise HTTPException(status_code=job_submission_response.status_code, detail=job_submission_response.text)


def poll_job_status(base_url, api_version, application, job_id, username, password, poll_interval, max_retries):
    job_status_url = f"{base_url}/rest/{api_version}/applications/{application}/jobs/{job_id}"
    job_status = -1  # Initialize with -1 (in progress)
    retry_count = 0  # Initialize retry count
    job_descriptive_status = "Unknown"  # Initialize descriptive status

    while job_status == -1 and retry_count < max_retries:
        response = requests.get(
            job_status_url,
            auth=(username, password)
        )
        if response.status_code == 200:
            job_details = response.json()
            job_status = job_details.get("status")
            job_descriptive_status = job_details.get("descriptiveStatus")
            logging.info(f"Job {job_id} status: {job_descriptive_status}")

            if job_status == -1:
                # If the job is still in progress, wait for the poll_interval before the next poll
                time.sleep(poll_interval)
                retry_count += 1  # Increment the retry count
            else:
                # Handle final job status (success, error, cancelled, etc.)
                logging.info(f"Final status for job {job_id}: {job_descriptive_status}")
                # You might want to store the final status in a database or send a notification
        else:
            # Log error and break the loop if unable to poll the job status
            logging.error(f"Error polling job status: {response.text}")
            break

    if retry_count >= max_retries:
        logging.error(f"Max retries exceeded for job {job_id}. Last known status: {job_descriptive_status}")