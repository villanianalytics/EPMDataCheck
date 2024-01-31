from fastapi import FastAPI, HTTPException, File, UploadFile, Form
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
import csv
import io
from io import StringIO
import pandas as pd
import os
import logging
import hashlib
import openpyxl
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
import time
import requests
from fastapi import FastAPI, Form, BackgroundTasks, HTTPException
import logging

import requests
import json
import numpy as np


logging.basicConfig(
    filename='application.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = FastAPI()


def json_to_csv(json_data):
    try:
        logging.info("Inside json_to_csv function")
        output = io.StringIO()
        csv_writer = csv.writer(output)

        # Determine the number of empty cells needed in the header
        first_row_headers = json_data.get('rows', [])[0].get('headers', [])
        empty_cells = len(first_row_headers)

        # Write the Column headers
        for col in json_data.get('columns', []):
            csv_writer.writerow([''] * empty_cells + col)

        # Write Rows
        for row in json_data.get('rows', []):
            headers = row.get('headers', [])
            data = row.get('data', [])
            csv_writer.writerow(headers + data)

        return output.getvalue()
    except Exception as e:
        logging.error(f"Error in json_to_csv: {e}")
        raise e


@app.post("/convert_json_to_csv/", response_class=PlainTextResponse)
async def convert_json_to_csv(json_payload: dict):
    try:
        logging.info("Received request to /convert_json_to_csv/")
        csv_data = json_to_csv(json_payload)
        return csv_data
    except Exception as e:
        logging.error(f"Error in /convert_json_to_csv/: {e}")
        raise HTTPException(status_code=400, detail=str(e))


def save_to_excel(data_pull_csv, comparison_csv, excel_path):
    try:
        # Read CSV data into pandas DataFrames
        data_pull_df = pd.read_csv(StringIO(data_pull_csv)).apply(pd.to_numeric, errors='ignore')
        comparison_df = pd.read_csv(StringIO(comparison_csv)).apply(pd.to_numeric, errors='ignore')

        # Check if both DataFrames are identical
        hash_pull = hashlib.sha256(data_pull_df.to_string().encode()).hexdigest()
        hash_comp = hashlib.sha256(comparison_df.to_string().encode()).hexdigest()

        match = hash_pull == hash_comp

        # Replace 'Unnamed' columns with empty string
        data_pull_df.columns = ['' if 'Unnamed' in str(col) else col for col in data_pull_df.columns]
        comparison_df.columns = ['' if 'Unnamed' in str(col) else col for col in comparison_df.columns]

        # Create an empty DataFrame for the variance with the same columns and indices
        variance_df = pd.DataFrame(index=comparison_df.index, columns=comparison_df.columns)

        # Populate the variance DataFrame with Excel formula placeholders
        for col in range(len(comparison_df.columns)):
            for row in comparison_df.index:
                col_letter = get_column_letter(col + 1)
                row_number = str(row + 2)
                variance_df.iat[row, col] = f"='Data Pull'!{col_letter}{row_number} = 'Comparison'!{col_letter}{row_number}"

        # Save to Excel
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            data_pull_df.to_excel(writer, sheet_name='Data Pull', index=False)
            comparison_df.to_excel(writer, sheet_name='Comparison', index=False)
            variance_df.to_excel(writer, sheet_name='Variance', index=False)

        return 200 if match else 412

    except Exception as e:
        logging.error(f"Error in save_to_excel: {e}")
        return 500


@app.post("/compare_csvs/")
async def compare_csvs(data_pull_file: UploadFile = File(...), comparison_file: UploadFile = File(...)):
    # Validate the uploaded files' type
    if data_pull_file.content_type not in ["application/vnd.ms-excel", "text/csv"]:
        return {"detail": "Invalid file type for data_pull_file"}, 400
    if comparison_file.content_type not in ["application/vnd.ms-excel", "text/csv"]:
        return {"detail": "Invalid file type for comparison_file"}, 400
    # Read the content of the uploaded files
    data_pull_csv = await data_pull_file.read()
    comparison_csv = await comparison_file.read()

    # Specify where to save the Excel output
    excel_path = "comparison.xlsx"

    # Call your save_to_excel function here
    status_code = save_to_excel(data_pull_csv.decode(), comparison_csv.decode(), excel_path)

    # Based on the returned status_code, send the appropriate response
    if status_code == 200:
        return FileResponse(excel_path, status_code=200)
    elif status_code == 412:
        return FileResponse(excel_path, status_code=412)
    else:
        return {"detail": "Internal Server Error"}, 500


@app.get("/")
def read_root():
    logging.info("Root URL was accessed.")  # This should also log to application.log
    return {"message": "Hello, World"}


@app.post("/export_data_slice_json/")
async def export_data_slice_json(
        base_url: str = Form(...),
        username: str = Form(...),
        password: str = Form(...),
        app_name: str = Form(...),
        api_version: str = Form(...),
        plan_type_name: str = Form(...),
        payload: str = Form(...)  # Accepting payload as a string
):
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


@app.post("/csv_to_json/")
async def csv_to_json(
        file: UploadFile = File(...),
        pov_dimensions: str = Form(...),
        pov_dimension_members: str = Form(...),
        col_dimensions: str = Form(...),
        row_dimensions: str = Form(...)):

    # Read the uploaded CSV file into a DataFrame
    content = await file.read()
    csv_content = StringIO(content.decode())
    df = pd.read_csv(csv_content, header=None)

    # Replace NaNs and Infs
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.fillna("", inplace=True)

    pov_dimensions_list = pov_dimensions.split(", ")
    pov_dimension_members_list = pov_dimension_members.split(", ")
    col_dimensions_list = col_dimensions.split(", ")
    row_dimensions_list = row_dimensions.split(", ")

    # Read column members
    col_members = []
    for col_index in range(len(row_dimensions_list), df.shape[1]):
        members_for_col = []
        for row_index in range(len(col_dimensions_list)):
            members_for_col.append([df.iloc[row_index, col_index]])
        col_members.append({"dimensions": col_dimensions_list, "members": members_for_col})

    # Read row members
    row_members = []
    for row_index in range(len(col_dimensions_list), df.shape[0]):
        members_for_row = []
        for col_index in range(len(row_dimensions_list)):
            members_for_row.append([df.iloc[row_index, col_index]])
        row_members.append({"dimensions": row_dimensions_list, "members": members_for_row})

    # Prepare the payload
    payload = {
        "exportPlanningData": False,
        "gridDefinition": {
            "suppressMissingBlocks": True,
            "suppressMissingRows": False,
            "suppressMissingColumns": False,
            "pov": {
                "dimensions": pov_dimensions_list,
                "members": [[m] for m in pov_dimension_members_list]
            },
            "columns": col_members,
            "rows": row_members
        }
    }

    return JSONResponse(content=payload)


@app.post("/import_data_slice_json/")
async def import_data_slice_json(
        base_url: str = Form(...),
        username: str = Form(...),
        password: str = Form(...),
        app_name: str = Form(...),
        api_version: str = Form(...),
        plan_type_name: str = Form(...),
        payload: str = Form(...)  # Accepting payload as a string or a file
):
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


@app.post("/run_job/")
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

async def poll_job_status(base_url, api_version, application, job_id, username, password, poll_interval, max_retries):
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

