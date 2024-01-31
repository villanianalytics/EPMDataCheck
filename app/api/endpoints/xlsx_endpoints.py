from starlette.responses import FileResponse

from app.core.xlsx_functions import *
from fastapi import APIRouter, HTTPException, Body, UploadFile, File

router = APIRouter()


@router.post("/compare_csvs/")
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
