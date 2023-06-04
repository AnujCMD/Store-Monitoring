from datetime import datetime, timedelta, time

import pytz
from dateutil.relativedelta import relativedelta

from config.mongo_connection import connect_mongo

# Triggers the connection to MongoDB
db = connect_mongo()

# Maximum timestamp based on the store_status
TIMESTAMP = '2023-01-25 18:13:22.480563 UTC'
# Global response dict based on
generated_report_response = {}


def generate_report(report_id):
    try:
        # Fetching all the store_status to fetch all the available store
        store_statuses = db.store_status_updated.find()
        store_id_dict = {}
        report_data = []
        for store in store_statuses:
            store_id = store['store_id']
            if store_id in store_id_dict:
                continue
            store_id_dict[store_id] = 1
            # Calculate the timezone of store, if timezone is not available take the default time as 'America/Chicago'
            store_timezone = db.store_timezone.find_one({"store_id": store_id})
            if store_timezone is None or store_timezone['timezone_str'] is None:
                timezone_str = 'America/Chicago'
            else:
                timezone_str = store_timezone['timezone_str']

            # Converting the current timestamp into datetime object
            current_timestamp = datetime.strptime(TIMESTAMP, "%Y-%m-%d %H:%M:%S.%f %Z")
            current_timestamp = current_timestamp.replace(tzinfo=pytz.UTC)

            # Fetching the store_status of store_id in last 7 days
            start_time = current_timestamp - relativedelta(days=7)
            store_status = db.store_status_updated.find({
                'store_id': store_id,
                'timestamp_utc': {'$gte': start_time, '$lt': current_timestamp}
            })
            store_status_list = []
            for status_value in store_status:
                store_status_list.append(status_value)

            # Calculate the store_timings for store_timing and converting it to UTC
            store_timing_dict = {}
            store_timings = db.store_timings.find({"store_id": store_id})
            for store_time in store_timings:
                local_tz = pytz.timezone(timezone_str)
                day = store_time['day']
                store_start_time = datetime.strptime(store_time['start_time_local'], "%H:%M:%S").time()
                local_datetime = datetime.combine(datetime.today(), store_start_time)
                local_datetime = local_tz.localize(local_datetime)
                store_start_time = local_datetime.astimezone(pytz.UTC)
                store_end_time = datetime.strptime(store_time['end_time_local'], "%H:%M:%S").time()
                local_datetime = datetime.combine(datetime.today(), store_end_time)
                local_datetime = local_tz.localize(local_datetime)
                store_end_time = local_datetime.astimezone(pytz.UTC)
                store_timing_dict[day] = [store_start_time, store_end_time]

            # Set the store timing as 24/7, if store_timings is not present in database
            if store_timing_dict.__len__() == 0:
                store_timing_dict[0] = '24/7'
            sorted_data = sorted(store_status_list, key=lambda x: x['timestamp_utc'])
            target_time = datetime(2023, 1, 25, 18, 13, 22, 480563)
            one_day_before = target_time - timedelta(days=1)
            one_hour_before = target_time - timedelta(hours=1)
            downtime_minutes = 0
            uptime_minutes = 0

            # Calculate the store status for last one hour in minutes
            index = None
            for i, record in enumerate(sorted_data):
                if record['timestamp_utc'] >= one_hour_before:
                    index = i
                    break
            if index is None:
                uptime_hours = -1
            if index is not None:
                # Calculate uptime_last_hour in minutes
                one_hour_status = [sorted_data[index - 1]]
                for record in sorted_data[index:]:
                    one_hour_status.append(record)
                for i in range(len(one_hour_status)):
                    status = one_hour_status[i]['status']
                    # Calculate the timestamp and the start and end time of store based on weekday of store_timings
                    timestamp = one_hour_status[i]['timestamp_utc']
                    day = timestamp.weekday()
                    time_arr = store_timing_dict.get(day)
                    if time_arr is None:
                        continue
                    # If store timing is 24/7, then setting the timing from 12 AM to 11:59 PM
                    if store_timing_dict.get(0) == '24/7':
                        start_time = time(0, 0)
                        dt_start = datetime.combine(datetime.today().date(), start_time)
                        end_time = time(23, 59)
                        dt_end = datetime.combine(datetime.today().date(), end_time)
                        time_arr[0] = dt_start
                        time_arr[1] = dt_end
                    start = time_arr[0]
                    end = time_arr[1]
                    downtime_flag = False
                    if i == 0:
                        continue
                    else:
                        previous_record = one_hour_status[i - 1]
                    if i == len(one_hour_status) - 1:
                        if i - 1 == 0 and start.time() <= current_timestamp.time() <= end.time():
                            if previous_record['status'] == 'active' and status == 'active':
                                uptime_minutes = 60
                                continue
                            elif previous_record['status'] == 'inactive' and status == 'inactive':
                                downtime_minutes = 60
                                continue
                        if status == 'inactive' and start.time() <= current_timestamp.time() <= end.time():
                            downtime_flag = True
                            time_difference = datetime.combine(datetime.min, target_time.time()) - datetime.combine(
                                datetime.min, timestamp.time())
                            minutes = time_difference.seconds // 60
                            downtime_minutes += minutes
                        if status == 'active' and start.time() <= current_timestamp.time() <= end.time():
                            time_difference = datetime.combine(datetime.min, target_time.time()) - datetime.combine(
                                datetime.min, timestamp.time())
                            minutes = time_difference.seconds // 60
                            uptime_minutes += minutes
                        continue
                    next_record = one_hour_status[i + 1]
                    previous_status = previous_record['status']
                    previous_time = previous_record['timestamp_utc']
                    next_status = next_record['status']
                    next_time = next_record['timestamp_utc']
                    if status == 'inactive' and start.time() <= current_timestamp.time() <= end.time():
                        downtime_flag = True
                        if previous_status == 'inactive':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, previous_time.time())
                            minutes = time_difference.seconds // 60
                            downtime_minutes += minutes
                            continue
                        elif previous_time == 'active':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, timestamp.time())
                            minutes = time_difference.seconds // 60
                            downtime_minutes += minutes
                            continue
                    if status == 'active' and start.time() <= current_timestamp.time() <= end.time():
                        if previous_status == 'inactive':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, timestamp.time())
                            minutes = time_difference.seconds // 60
                            downtime_minutes += minutes
                            continue
                        elif previous_time == 'active':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, previous_time.time())
                            minutes = time_difference.seconds // 60
                            downtime_minutes += minutes
                            continue
                    # Set the uptime as 60 if there is no downtime in last hour
                    if downtime_flag is False and uptime_minutes != 60:
                        uptime_minutes = 60

            # Calculate the uptime/downtime(in hours) in last one day
            index = None
            downtime_hours = 0
            uptime_hours = 0
            # Calculate the index of document which is one day before the current timestamp
            for i, record in enumerate(sorted_data):
                if record['timestamp_utc'] >= one_day_before:
                    index = i
                    break
            # If data is not present, mark the uptime_hours as -1
            if index is None:
                uptime_hours = -1
            if index is not None:
                one_day_status = [sorted_data[index - 1]]
                for record in sorted_data[index:]:
                    one_day_status.append(record)
                # Calculate the timestamp and the start and end time of store based on weekday of store_timings
                timestamp = one_day_status[1]['timestamp_utc']
                if timestamp is None:
                    continue
                day = timestamp.weekday()
                time_arr = store_timing_dict.get(day)
                # Skip if timestamp is not available, fallback
                if time_arr is None:
                    continue
                # If store timing is 24/7, then setting the timing from 12 AM to 11:59 PM
                if store_timing_dict.get(0) == '24/7':
                    start_time = time(0, 0)
                    dt_start = datetime.combine(datetime.today().date(), start_time)
                    end_time = time(23, 59)
                    dt_end = datetime.combine(datetime.today().date(), end_time)
                    time_arr[0] = dt_start
                    time_arr[1] = dt_end
                start = time_arr[0]
                end = time_arr[1]
                time_difference = datetime.combine(datetime.min, start.time()) - datetime.combine(
                    datetime.min, end.time())
                hours = time_difference.seconds // 3600
                uptime_hours = hours
                downtime_flag = False
                # Iterate through the data and set the uptime hours in previous day
                for i in range(len(one_day_status)):
                    status = one_day_status[i]['status']
                    if i == 0:
                        continue
                    else:
                        previous_record = one_day_status[i - 1]
                    if i == len(one_day_status) - 1:
                        if i - 1 == 0 and start.time() <= current_timestamp.time() <= end.time():
                            if previous_record['status'] == 'active' and status == 'active':
                                uptime_hours = hours
                                continue
                            elif previous_record['status'] == 'inactive' and status == 'inactive':
                                downtime_hours = hours
                                continue
                        if status == 'inactive' and start.time() <= current_timestamp.time() <= end.time():
                            downtime_minutes += 1
                            uptime_hours -= 1
                        continue
                    next_record = one_day_status[i + 1]
                    previous_status = previous_record['status']
                    previous_time = previous_record['timestamp_utc']
                    next_time = next_record['timestamp_utc']
                    if status == 'inactive' and start.time() <= current_timestamp.time() <= end.time():
                        downtime_flag = True
                        if previous_status == 'inactive':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, previous_time.time())
                            hours = time_difference.seconds // 3600
                            downtime_hours += hours
                            uptime_hours -= hours
                            continue
                        elif previous_time == 'active':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, timestamp.time())
                            hours = time_difference.seconds // 3600
                            downtime_hours += hours
                            uptime_hours -= hours
                            continue
                    if downtime_flag is False and uptime_hours != hours:
                        uptime_hours = hours
            total_day = 7
            itr = 0
            limit = 0
            downtime_days = 0
            uptime_days = 0
            week_data_flag = False

            # Calculate the uptime/downtime in last 7 days
            while total_day > 0:
                for i, record in enumerate(sorted_data):
                    week_data_flag = True
                    time_st = target_time - timedelta(days=total_day - 1)
                    if record['timestamp_utc'] >= time_st:
                        limit = i
                        break
                if itr < len(sorted_data):
                    first_record = sorted_data[itr]
                    timestamp = first_record['timestamp_utc']
                    last_record = sorted_data[limit - 1]
                    day = timestamp.weekday()
                    # Calculate the store start and end time based on timestamp
                    time_arr = store_timing_dict.get(day)
                    if time_arr is None:
                        total_day -= 1
                        continue
                    # If store timing is 24/7, then setting the timing from 12 AM to 11:59 PM
                    if store_timing_dict.get(0) == '24/7':
                        start_time = time(0, 0)
                        dt_start = datetime.combine(datetime.today().date(), start_time)
                        end_time = time(23, 59)
                        dt_end = datetime.combine(datetime.today().date(), end_time)
                        time_arr[0] = dt_start
                        time_arr[1] = dt_end
                    start = time_arr[0]
                    end = time_arr[1]
                    if first_record['status'] == 'inactive' or last_record['status'] == 'inactive':
                        if start.time() <= timestamp <= end.time():
                            downtime_days += 1
                    elif first_record['status'] == 'active' or last_record['status'] == 'active':
                        uptime_days += 1
                total_day -= 1
            if week_data_flag is False:
                uptime_days = -1

            # Appending the data into report_data
            report_data.append({
                'report_id': report_id,
                'store_id': store_id,
                'uptime_last_hour': uptime_minutes,
                'uptime_last_day': uptime_hours,
                'uptime_last_week': uptime_days,
                'downtime_last_hour': downtime_minutes,
                'downtime_last_day': downtime_hours,
                'downtime_last_week': downtime_days
            })
            # Add the data to the response
            generated_report_response[report_id]['data'] = report_data
        # Mark the response as completed when the loop ends
        generated_report_response[report_id]['status'] = 'Completed'

    except Exception as e:
        generated_report_response[report_id]["status"] = "failed to generate the report"
        generated_report_response[report_id]['erorr'] = e


def update_report():
    store_statuses = db.store_status.find()
    report = []
    total_error = 0
    try:
        for store in store_statuses:
            timestamp = store['timestamp_utc']
            timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f %Z")
            timestamp_dt = timestamp_dt.replace(tzinfo=pytz.UTC)
            store_id = store['store_id']
            status = store['status']
            timestamp_utc = store['timestamp_utc']
            report.append({
                "store_id": store_id,
                "status": status,
                "timestamp_utc": timestamp_dt
            })
    except ValueError as e:
        print("Error:", e)
        total_error = total_error + 1
    print(f"Total Error encountered while updating timestamp: {total_error}")
    db.store_status_updated.insert_many(report)
