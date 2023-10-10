from pydantic import BaseModel
from datetime import datetime
from typing import List


class StoreHours(BaseModel):
    store_id: int
    day: int
    start_time_local: str
    end_time_local: str


class StoreStatus(BaseModel):
    store_id: int
    status: str
    timestamp_utc: datetime


class TimeZone(BaseModel):
    store_id: int
    timezone_str: str


class ReportData(BaseModel):
    store_id: int = 0
    uptime_last_hour: int = 0
    uptime_last_day: int = 0
    uptime_last_week: int = 0
    downtime_last_hour: int = 0
    downtime_last_day: int = 0
    downtime_last_week: int = 0


class Report(BaseModel):
    status: str = 'Running'
    data: List[ReportData] = None
