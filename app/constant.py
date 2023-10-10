from datetime import datetime

DB_URL = "mongodb://test:test123@mongodb:27017/resturant_details"

START_TIME = datetime.strptime(
    '00:00:00', "%H:%M:%S").time()
END_TIME = datetime.strptime(
    '23:59:59', "%H:%M:%S").time()
MISSING_TIMEZONE = 'America/Chicago'
