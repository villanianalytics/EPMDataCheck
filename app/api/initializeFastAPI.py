from fastapi import FastAPI
import uvicorn
from fastapi import FastAPI
from app.core.logging_engine import *

from app.api.endpoints import json_endpoints  # adjust this import based on your actual path
from app.api.endpoints import csv_endpoints
from app.api.endpoints import epm_endpoints
from app.api.endpoints import xlsx_endpoints


def init_fastapi():
    app = FastAPI()
    app.include_router(json_endpoints.router, prefix="/json", tags=["json"])
    app.include_router(csv_endpoints.router, prefix="/csv", tags=["csv"])
    app.include_router(epm_endpoints.router, prefix="/epm", tags=["epm"])
    app.include_router(xlsx_endpoints.router, prefix="/xlsx", tags=["xlsx"])
    uvicorn.run(app, host="0.0.0.0", port=8000)
