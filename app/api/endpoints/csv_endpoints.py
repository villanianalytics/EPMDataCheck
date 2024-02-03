from starlette.responses import PlainTextResponse

from app.core.csv_functions import *
from fastapi import APIRouter, HTTPException, Body, UploadFile, File, Form

router = APIRouter()


@router.post("/csv_to_json/")
async def convert_csv_to_json(file: UploadFile = File(...),
                              pov_dimensions: str = Form(...),
                              pov_dimension_members: str = Form(...),
                              col_dimensions: str = Form(...),
                              row_dimensions: str = Form(...)):
    try:
        logging.info("Received request to /convert_csv_to_json/")
        csv_data = csv_to_json(
            file,
            pov_dimensions,
            pov_dimension_members,
            col_dimensions,
            row_dimensions)
        return csv_data
    except Exception as e:
        logging.error(f"Error in /convert_csv_to_json/: {e}")
        raise HTTPException(status_code=400, detail=str(e))
