from fastapi import FastAPI, UploadFile, File, HTTPException, status, BackgroundTasks
from app.db import Store_Status, Store_TimeZone, Store_Working_Hour, Report_Data
import pandas as pd
from fastapi.encoders import jsonable_encoder
import logging
from app.schema import Report
from app.constant import START_TIME, END_TIME, MISSING_TIMEZONE
from dateutil import parser
import datetime
from datetime import timezone, timedelta
import pytz
app = FastAPI()
# Set up a logger with basic configuration
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()


reports = []
last_hour_uptime = -1
last_day_uptime = -1
last_week_uptime = 0
downtime_last_hour = -1
downtime_last_day = -1
downtime_last_week = 0
work_hours = dict()


def parse_datetime(row, zone):
    parsed_dt = parser.parse(row['timestamp_utc'])
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    parsed_dt = (parsed_dt.astimezone(
        pytz.timezone(zone))).replace(microsecond=0, tzinfo=None)
    row['date'] = parsed_dt.date()
    row['timestamp_utc'] = parsed_dt.time()
    return row


async def store_data(data_list, cursor):
    chunksize = 20_000  # Adjust chunk size based on your requirements
    # Iterate over CSV chunks and append to DataFrame
    for chunk in pd.read_csv(data_list, chunksize=chunksize, encoding='utf-8'):
        result = cursor.insert_many(chunk.to_dict(orient='records'))
        logger.info("added data!!! " + str(len(result.inserted_ids)))


def get_store_hours(store_id: str):
    global work_hours
    store_times = Store_Working_Hour.find({'store_id': store_id}, {
                                          '_id': 0, 'day': 1, 'start_time_local': 1, 'end_time_local': 1})
    for store_time in store_times:
        start_time = datetime.datetime.strptime(store_time['start_time_local'], "%H:%M:%S").time()
        end_time = datetime.datetime.strptime(store_time['end_time_local'], "%H:%M:%S").time()
        if store_time['day'] in work_hours:
            list_time = work_hours[store_time['day']]
            list_time.append((datetime.datetime.strptime(store_time['start_time_local'], "%H:%M:%S").time(), datetime.datetime.strptime(
                store_time['end_time_local'], "%H:%M:%S").time()))
            list_time.sort()
            work_hours[store_time['day']] = list_time
        else:
            work_hours[store_time['day']] = [(datetime.datetime.strptime(
                store_time['start_time_local'], "%H:%M:%S").time(), datetime.datetime.strptime(store_time['end_time_local'], "%H:%M:%S").time())]


def check_in_store_hours(day, time):
    global work_hours
    is_within_any_period = False
    min_time = END_TIME
    if day in work_hours:
        time_list = work_hours[day]
        for start, end in time_list:
            if start <= time <= end:
                is_within_any_period = True
                min_time = min(min_time, end)
        return is_within_any_period, min_time
    else:
        return True, min_time


def get_system_runtime(day):
    global work_hours
    total_time = 0
    if day in work_hours:
        for start, end in work_hours[day]:
            time = ((end.hour - start.hour) * 60 +
                    (end.minute - start.minute))//60
            if time == 23:
                time = time + 1
            total_time = total_time + time
    else:
        print(day, end= " ")
        return 24

    return total_time


def generate_for_single_status(status):
    global last_day_uptime, last_hour_uptime, last_week_uptime, downtime_last_day, downtime_last_hour, downtime_last_week, work_hours
    # print(downtime_last_hour, downtime_last_day,downtime_last_week, last_hour_uptime,last_day_uptime,last_week_uptime)
    print(work_hours)
    total_time_week = 0
    total_time_day = -1
    # print(status)
    check = []
    for i in range(0, 7):
        time = get_system_runtime(i)
        total_time_week = total_time_week + time
        check.append(time)
        if total_time_day == -1:
            total_time_day = total_time_week

    if status == 'active':
        last_hour_uptime = 60
        last_day_uptime = total_time_day
        last_week_uptime = last_week_uptime + total_time_week
        downtime_last_hour = 0
        downtime_last_day = 0
        downtime_last_week = 0
    else:
        downtime_last_hour = 60
        downtime_last_day = total_time_day
        downtime_last_week = total_time_week
        last_hour_uptime = 0
        last_day_uptime = 0
        last_week_uptime = 0
    print(check)
    # print(downtime_last_hour, downtime_last_day,downtime_last_week, last_hour_uptime,last_day_uptime,last_week_uptime)


def generate_for_multi_status(data):
    global last_day_uptime, last_hour_uptime, last_week_uptime, downtime_last_day, downtime_last_hour, downtime_last_week

    statuses = data['status'].unique()
    day = data.iloc[0]['date'].weekday()
    end_date = (work_hours[day])[-1][1]
    end_date = datetime.time(end_date.hour-1, end_date.minute)
    time_value = get_system_runtime(day)
    if len(statuses) == 1:
        # print(data)
        # print(time_value)
        if statuses[0] == 'active':
            if last_day_uptime == -1:
                last_day_uptime = time_value
                last_hour_uptime = 60
                downtime_last_hour = 0
                downtime_last_day = 0
            last_week_uptime = last_week_uptime + time_value
        else:
            if downtime_last_day == -1:
                downtime_last_day = time_value
                last_day_uptime = 0
                downtime_last_hour = 60
                last_hour_uptime = 0
            downtime_last_week = downtime_last_week + time_value
    else:
        prev = -1
        downtime = 0
        for indx, row in data.iterrows():
            time = row['timestamp_utc']
            check, prev_time = check_in_store_hours(day, time)
            # print(row['status'], time, check, prev_time, prev)
            if check:
                if row['status'] == 'inactive':
                    if time < end_date:
                        if downtime_last_hour == -1 and last_hour_uptime == -1:
                            downtime_last_hour = downtime
                            last_hour_uptime = 60 - downtime
                    if prev == -1:
                        downtime = downtime + \
                            ((prev_time.hour - time.hour) * 60 +
                             (prev_time.minute - time.minute))
                        # print(downtime)
                    else:
                        d = ((prev_time.hour - time.hour) * 60 +
                             (prev_time.minute - time.minute))
                        d1 = ((prev.hour - time.hour) * 60 +
                              (prev.minute - time.minute))
                        downtime = downtime + min(d, d1)
                        # print(min(d, d1))
                        # print(downtime)
                prev = time

        downtime = downtime//60
        if downtime_last_day == -1:
            downtime_last_day = downtime
        if last_day_uptime == -1:
            last_day_uptime = time_value - downtime
        downtime_last_week = downtime_last_week + downtime
        last_week_uptime = last_week_uptime + (time_value - downtime)


def generate_report():
    global last_day_uptime, last_hour_uptime, last_week_uptime, downtime_last_day, downtime_last_hour, downtime_last_week
    store_ids = Store_Status.distinct('store_id')
    for store_id in store_ids[:15]:
        last_hour_uptime = -1
        last_day_uptime = -1
        last_week_uptime = 0
        downtime_last_hour = -1
        downtime_last_day = -1
        downtime_last_week = 0
        # store_id=7270851084212859258
        chunk = Store_Status.find({'store_id': store_id}, {
                                  '_id': 0, 'status': 1, 'timestamp_utc': 1})
        data = pd.DataFrame(chunk)
        statuses = data['status'].unique()
        if len(statuses) == 1:
            
            generate_for_single_status(statuses[0])
        else:
            store_zone = Store_TimeZone.find_one(
                {'store_id': store_id}, {'_id': 0, 'timezone_str': 1})
            get_store_hours(store_id=store_id)
            # print(work_hours)
            if store_zone is None:
                store_zone = MISSING_TIMEZONE
            else:
                store_zone = store_zone['timezone_str']

            data = data.apply(parse_datetime, args=(
                store_zone,), axis=1)

            data.sort_values(by=['date', 'timestamp_utc'], ascending=False,
                             inplace=True, ignore_index=True)
            end_date = data['date'].max() - timedelta(7)
            # print(end_date)
            data.groupby(by='date', sort=False).apply(
                lambda x: generate_for_multi_status(x) if x.iloc[0]['date'] >= end_date else None)

        report = {
            'store_id': store_id,
            'uptime_last_hour': last_hour_uptime,
            'uptime_last_day': last_day_uptime,
            'uptime_last_week': last_week_uptime,
            'downtime_last_hour': downtime_last_hour,
            'downtime_last_day': downtime_last_day,
            'downtime_last_week': downtime_last_week
        }
        print(report, "\n")
        reports.append(report)
    
    res = Report_Data.insert_one(jsonable_encoder(Report(
        status='Completed',
        data=reports
    )))


@app.get("/")
def root():
    return {"message": "Server running"}


@app.post("/status/add")
async def add_store_status(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if file.content_type != 'text/csv':
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail="Only csv files are allowed!!!",
        )
    background_tasks.add_task(store_data, file.file, Store_Status)

    return {"message": file.filename + " data processed successfully."}


@app.post("/time/add")
async def add_store_status(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if file.content_type != 'text/csv':
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail="Only csv files are allowed!!!",
        )
    background_tasks.add_task(store_data, file.file, Store_TimeZone)

    return {"message": file.filename + " data processed successfully."}


@app.post("/hours/add/")
async def add_or_update_store_hours(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if file.content_type != 'text/csv':
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail="Only csv files are allowed!!!",
        )
    background_tasks.add_task(store_data, file.file, Store_Working_Hour)

    return {"message": file.filename + " data getting stored"}


@app.get("/trigger_report")
async def compute_report(background_tasks: BackgroundTasks):
    # await generate_report()
    result = Report_Data.insert_one(jsonable_encoder(Report()))
    background_tasks.add_task(generate_report)
    return {'report_id': str(result.inserted_id)}
