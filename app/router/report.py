from fastapi import HTTPException, status, BackgroundTasks, Response, APIRouter
from app.db import Store_Status, Store_TimeZone, Store_Working_Hour
from io import StringIO
import pandas as pd
import logging
from app.constant import END_TIME, MISSING_TIMEZONE
from dateutil import parser
import datetime
import random
from datetime import timezone, timedelta
import pytz
import csv
# Set up a logger with basic configuration
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger()

router = APIRouter()

# list to hold all store reports
reports = []

# to keep track of the requested report_id
report_ids = set()

# to set the report generation status
report_status = ""


# global variables for calculation
last_hour_uptime = -1
last_day_uptime = -1
last_week_uptime = 0
downtime_last_hour = -1
downtime_last_day = -1
downtime_last_week = 0


# to change the utc timestamp to respective timezone
def parse_datetime(row, zone):
    parsed_dt = parser.parse(row['timestamp_utc'])
    if parsed_dt.tzinfo is None:
        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
    parsed_dt = (parsed_dt.astimezone(
        pytz.timezone(zone))).replace(microsecond=0, tzinfo=None)
    row['date'] = parsed_dt.date()
    row['timestamp_utc'] = parsed_dt.time()
    return row


# creates a dictionary of store weekly working hours
def get_store_hours(store_id: str):
    work_hours = dict()
    store_times = Store_Working_Hour.find({'store_id': store_id}, {
                                          '_id': 0, 'day': 1, 'start_time_local': 1, 'end_time_local': 1})
    for store_time in store_times:
        start_time = datetime.datetime.strptime(
            store_time['start_time_local'], "%H:%M:%S").time()
        end_time = datetime.datetime.strptime(
            store_time['end_time_local'], "%H:%M:%S").time()
        if store_time['day'] in work_hours:
            # filtering overlaping intervals
            intervals = work_hours[store_time['day']]
            new_intervals = []
            for interval_start, interval_end in intervals:
                if (end_time < interval_start):
                    new_intervals.append((start_time, end_time))
                    start_time, end_time = interval_start, interval_end
                elif (start_time > interval_end):
                    new_intervals.append((interval_start, interval_end))
                else:
                    start_time = min(start_time, interval_start)
                    end_time = max(end_time, interval_end)
            new_intervals.append((start_time, end_time))
            work_hours[store_time['day']] = new_intervals
        else:
            work_hours[store_time['day']] = [(datetime.datetime.strptime(
                store_time['start_time_local'], "%H:%M:%S").time(), datetime.datetime.strptime(store_time['end_time_local'], "%H:%M:%S").time())]

    return work_hours


# check wheather the timestamp is in working hours of the store
def check_in_store_hours(day, time, work_hours):
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


# get the total working hour of a day for a store
def get_system_runtime(day, work_hours):
    total_time = 0
    if day in work_hours:
        for start, end in work_hours[day]:
            time = ((end.hour - start.hour) * 60 +
                    (end.minute - start.minute))
            if time == 1439:
                time = time + 1
            total_time = total_time + time
    else:
        return 24

    return total_time/60


def generate_for_single_status(status, store_hours):
    """Calculating the uptime and downtime for stores which have all 
    timestamp status as either active or inactive in the dataset for a week.
    If the status remains the same for all the provided timestamps, 
    it is presumed to be consistent for timestamps that were not explicitly provided. 
    It helps to reduce extra calculations."""

    global last_day_uptime, last_hour_uptime, last_week_uptime, downtime_last_day, downtime_last_hour, downtime_last_week
    total_time_week = 0
    total_time_day = -1

    # calculating the total expected uptime for the store based on the work hours dictionary
    for i in range(0, 7):
        time = get_system_runtime(i, store_hours)
        total_time_week = total_time_week + time
        if total_time_day == -1:
            total_time_day = total_time_week

    """
    Assuming if the status was active then the store was active all the time, 
    else it was inactive all the time.
    """
    if status == 'active':
        last_hour_uptime = 60
        last_day_uptime = total_time_day
        last_week_uptime = total_time_week
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


# calculating uptime and downtime for stores which were both active and inactive on a day
def generate_for_multi_status(data, store_hours):
    global last_day_uptime, last_hour_uptime, last_week_uptime, downtime_last_day, downtime_last_hour, downtime_last_week

    """assuming that the store should be active in the working hours and 
    calculating downtime from the given data for a day, then subtracting it with total store hours to get the uptime."""
    # getting the status for the store on a day 
    statuses = data['status'].unique()
    
    # setting the last hour limit from the store closing time to calulation uptime/downtime for last hour
    day = data.iloc[0]['date'].weekday()
    end_date = None
    start_date = None
    if day in store_hours:
        start_date = (store_hours[day])[-1][1]
        end_date = datetime.time(start_date.hour-1, start_date.minute)
    else:
        start_date = datetime.time(23, 59)
        end_date = datetime.time(22, 59)
    
    # getting the total working hour of the store for a day
    time_value = get_system_runtime(day, store_hours)

    """
    Assuming if the status for all provided timestamps were active 
    then the store was active all day and vice versa.
    """
    if len(statuses) == 1:
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
    # if the store has both active and inactive status
    else:
        prev = -1
        downtime = 0
        downtime_hour = -1
        for indx, row in data.iterrows():
            time = row['timestamp_utc']
            check, prev_time = check_in_store_hours(day, time, store_hours) # checking whether the timestamp is in store hours
            if check:
                if row['status'] == 'inactive':
                    if prev == -1:
                        downtime = downtime + \
                            ((prev_time.hour - time.hour) * 60 +
                             (prev_time.minute - time.minute))
                    else:
                        d = ((prev_time.hour - time.hour) * 60 +
                             (prev_time.minute - time.minute))
                        d1 = ((prev.hour - time.hour) * 60 +
                              (prev.minute - time.minute))
                        downtime = downtime + min(d, d1)
                prev = time
                # calculating the downtime for last hour 
                if end_date < time <= start_date and downtime_last_hour == -1 and last_hour_uptime == -1:
                    downtime_hour = downtime_hour + downtime

        downtime = downtime/60
        # calculating uptime/downtime for a last hour
        if downtime_last_hour == -1 and last_hour_uptime == -1:
            if downtime_hour == -1:
                if data.iloc[0]['status'] == 'active':
                    last_hour_uptime = 60
                    downtime_last_hour = 0
                else:
                    last_hour_uptime = 0
                    downtime_last_hour = 60
            else:
                downtime_hour = downtime_hour + 1
                last_hour_uptime = 60 - downtime_hour
                downtime_last_hour = downtime_hour
        # calculating uptime/downtime for a last day
        if downtime_last_day == -1 and last_day_uptime == -1:
            downtime_last_day = downtime
            last_day_uptime = time_value - downtime

        downtime_last_week = downtime_last_week + downtime
        last_week_uptime = last_week_uptime + (time_value - downtime)


# start point of the calulation of uptime/downtime of stores.
def generate_report():
    global last_day_uptime, last_hour_uptime, last_week_uptime, downtime_last_day, downtime_last_hour, downtime_last_week, report_status
    store_ids = Store_Status.distinct('store_id')
    for store_id in store_ids:
        last_hour_uptime = -1
        last_day_uptime = -1
        last_week_uptime = 0
        downtime_last_hour = -1
        downtime_last_day = -1
        downtime_last_week = 0
        chunk = Store_Status.find({'store_id': store_id}, {
                                  '_id': 0, 'status': 1, 'timestamp_utc': 1})
        data = pd.DataFrame(chunk)
        store_hours = get_store_hours(store_id=store_id)
        print(store_hours)
        statuses = data['status'].unique()
        if len(statuses) == 1:
            generate_for_single_status(statuses[0], store_hours)
        else:
            store_zone = Store_TimeZone.find_one(
                {'store_id': store_id}, {'_id': 0, 'timezone_str': 1})

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
                lambda x: generate_for_multi_status(x, store_hours) if x.iloc[0]['date'] >= end_date else None)
        report = {
            'store_id': store_id,
            'uptime_last_hour': last_hour_uptime,
            'uptime_last_day': round(last_day_uptime, 2),
            'uptime_last_week': round(last_week_uptime, 2),
            'downtime_last_hour': round(downtime_last_hour, 2),
            'downtime_last_day': round(downtime_last_day, 2),
            'downtime_last_week': round(downtime_last_week, 2)
        }
        logger.info(report)
        reports.append(report)

    report_status = "Completed"


def generate_random_number():
    # Generate a random 10-digit number
    return ''.join([str(random.randint(0, 9)) for i in range(10)])


@router.get("/trigger_report")
async def compute_report(background_tasks: BackgroundTasks):
    global report_status
    report_id = generate_random_number()
    report_status = "Running"
    report_ids.add(report_id)
    background_tasks.add_task(generate_report)
    return {'report_id': report_id}


@router.get("/get_report endpoint/{item_id}")
async def get_report(item_id: str):
    global reports

    if item_id not in report_ids:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="The report id is not valid!!!"
        )

    if report_status == "Completed":
        field_names = ["store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week",
                       "downtime_last_hour", "downtime_last_day", "downtime_last_week"]
        csv_buffer = StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=field_names)

        # Write the CSV header
        writer.writeheader()

        # Write each row from the data
        for row in reports:
            writer.writerow(row)

        # Get the CSV content as a string
        csv_content = csv_buffer.getvalue()

        # Set response headers
        response = Response(content=csv_content)
        response.headers["Content-Disposition"] = "attachment; filename=report.csv"
        response.headers["Content-Type"] = "text/csv"

        # Add a completion message in the response
        response.headers["X-Message"] = "Report generation completed"

        return response
    return {
        "status": report_status
    }
