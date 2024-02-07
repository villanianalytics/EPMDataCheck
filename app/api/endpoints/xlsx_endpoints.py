from typing import List
from urllib import request

from starlette.responses import FileResponse, JSONResponse

from app.core.xlsx_functions import *
from fastapi import APIRouter, HTTPException, Body, UploadFile, File, Form

router = APIRouter()


@router.post("/variance_csvs/")
async def create_variance_csvs(
    data_pull_csv: UploadFile = File(...),
    comparison_csv: UploadFile = File(...),
    excel_path: str = Form(...),
    src_num_headers: int = Form(...),
    tgt_num_headers: int = Form(...),
    row_dimensions: List[str] = Form(...)  # This captures multiple row_dimensions form fields as a list
    ):
    # Read the content of the uploaded files
    try:
        data_pull_content = await data_pull_csv.read()
        comparison_content = await comparison_csv.read()

        # You would typically save these files to disk or process them directly from memory
        # For this example, we assume processing directly from memory, hence converting content to string
        # Be sure to adjust your save_to_excel_with_hash_check function to work with file content or paths as necessary

        # Assuming save_to_excel_with_hash_check is adjusted to work with the file content
        status_code = save_to_excel_with_hash_check(
            data_pull_content.decode('utf-8'),  # Decoding bytes to string
            comparison_content.decode('utf-8'),  # Decoding bytes to string
            excel_path,
            src_num_headers,
            tgt_num_headers,
            row_dimensions
        )
        return JSONResponse(status_code=200, content={"message": "Success", "status_code": status_code})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # It's important to close the files to free up resources
        await data_pull_csv.close()
        await comparison_csv.close()
