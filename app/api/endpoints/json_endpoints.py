from starlette.responses import PlainTextResponse

from app.core.json_functions import *
from fastapi import APIRouter, HTTPException, Body
router = APIRouter()


@router.post("/convert_json_to_csv/", response_class=PlainTextResponse)
async def convert_json_to_csv(json_payload: dict):
    try:
        logging.info("Received request to /convert_json_to_csv/")
        csv_data = json_to_csv(json_payload)
        return csv_data
    except Exception as e:
        logging.error(f"Error in /convert_json_to_csv/: {e}")
        raise HTTPException(status_code=400, detail=str(e))
