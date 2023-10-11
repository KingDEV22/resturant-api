from fastapi import APIRouter, status, HTTPException, UploadFile, File
from app.db import Store_Status, Store_TimeZone, Store_Working_Hour
import logging
import pandas as pd

# Set up a logger with basic configuration
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

router = APIRouter()

# add data to the database
async def store_data(data_list, cursor):
    chunksize = 50_000  # Adjust chunk size based on your requirements
    # Iterate over CSV chunks and append to DataFrame
    for chunk in pd.read_csv(data_list, chunksize=chunksize, encoding='utf-8'):
        result = cursor.insert_many(chunk.to_dict(orient='records'))
        logger.info("added data!!! " + str(len(result.inserted_ids)))


@router.post("/status/add")
async def add_store_status(file: UploadFile = File(...)):
    if file.content_type != 'text/csv':
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail="Only csv files are allowed!!!",
        )
    await store_data(file.file, Store_Status)
    return {"message": file.filename + " data stored successfully"}


@router.post("/time/add")
async def add_store_status(file: UploadFile = File(...)):
    if file.content_type != 'text/csv':
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail="Only csv files are allowed!!!",
        )
    await store_data(file.file, Store_TimeZone)
    return {"message": file.filename + " data stored successfully"}


@router.post("/hours/add/")
async def add_or_update_store_hours(file: UploadFile = File(...)):
    if file.content_type != 'text/csv':
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail="Only csv files are allowed!!!",
        )
    await store_data(file.file, Store_Working_Hour)
    return {"message": file.filename + " data stored successfully"}
