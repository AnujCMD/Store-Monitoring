from datetime import datetime, timedelta, time

import pytz
from dateutil.relativedelta import relativedelta

from config.mongo_connection import connect_mongo
from main import generated_report_response

db = connect_mongo()
# Define the business hours for a store (9 AM to 12 PM on Monday)
business_hours_start = datetime.strptime('09:00:00', '%H:%M:%S').time()
business_hours_end = datetime.strptime('12:00:00', '%H:%M:%S').time()
TIMESTAMP = '2023-01-25 18:13:22.480563 UTC'
# number_week_relation = {
#     0: 'Monday',
#     1: 'Tuesday',
#     2: 'Wednesday',
#     3: 'Thursday',
#     4: 'Friday',
#     5: 'Saturday',
#     6: 'Sunday'
# }


def generate_report(report_id):
    try:
        uptime_minutes = 0
        uptime_hours = 0
        uptime_days = 0

        downtime_minutes = 0
        downtime_hours = 0
        downtime_days = 0

        store_statuses = db.store_status_updated.find()
        report_data = []
        for store in store_statuses:
            store_id = store['store_id']
            store_timezone = db.store_timezone.find_one({"store_id": store_id})
            if store_timezone is None or store_timezone['timezone_str'] is None:
                timezone_str = 'America/Chicago'
            else:
                timezone_str = store_timezone['timezone_str']

            current_timestamp = datetime.strptime(TIMESTAMP, "%Y-%m-%d %H:%M:%S.%f %Z")
            current_timestamp = current_timestamp.replace(tzinfo=pytz.UTC)
            start_time = current_timestamp - relativedelta(days=7)
            store_status = db.store_status_updated.find({
                'store_id': store_id,
                'timestamp_utc': {'$gte': start_time, '$lt': current_timestamp}
            })
            store_status_list = []
            for status_value in store_status:
                store_status_list.append(status_value)

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
            if store_timing_dict.__len__() == 0:
                store_timing_dict[0] = '24/7'
            sorted_data = sorted(store_status_list, key=lambda x: x['timestamp_utc'])
            target_time = datetime(2023, 1, 25, 18, 13, 22, 480563)
            one_day_before = target_time - timedelta(days=1)
            one_hour_before = target_time - timedelta(hours=1)
            index = None
            for i, record in enumerate(sorted_data):
                if record['timestamp_utc'] >= one_hour_before:
                    index = i
                    break
            if index is None:
                uptime_hours = -1
            if index is not None:
                # Calculate uptime_last_hour in minutes
                one_hour_status = [sorted_data[index-1]]
                for record in sorted_data[index:]:
                    one_hour_status.append(record)
                for i in range(len(one_hour_status)):
                    status = one_hour_status[i]['status']
                    timestamp = one_hour_status[i]['timestamp_utc']
                    day = timestamp.weekday()
                    time_arr = store_timing_dict.get(day)
                    if time_arr is None:
                        continue
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
                        previous_record = one_hour_status[i-1]
                    if i == len(one_hour_status)-1:
                        if i-1 == 0 and start.time() <= current_timestamp.time() <= end.time():
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
                    next_record = one_hour_status[i+1]
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
                    if downtime_flag is False and uptime_minutes != 60:
                        uptime_minutes = 60
            index = None
            for i, record in enumerate(sorted_data):
                if record['timestamp_utc'] >= one_day_before:
                    index = i
                    break
            if index is None:
                uptime_hours = -1
            if index is not None:
                one_day_status = [sorted_data[index-1]]
                for record in sorted_data[index:]:
                    one_day_status.append(record)
                timestamp = one_day_status[1]['timestamp_utc']
                if timestamp is None:
                    continue
                day = timestamp.weekday()
                time_arr = store_timing_dict.get(day)
                if time_arr is None:
                    continue
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
                for i in range(len(one_day_status)):
                    status = one_day_status[i]['status']
                    if i == 0:
                        continue
                    else:
                        previous_record = one_day_status[i-1]
                    if i == len(one_day_status)-1:
                        if i-1 == 0 and start.time() <= current_timestamp.time() <= end.time():
                            if previous_record['status'] == 'active' and status == 'active':
                                continue
                            elif previous_record['status'] == 'inactive' and status == 'inactive':
                                continue
                        if status == 'inactive' and start.time() <= current_timestamp.time() <= end.time():
                            downtime_minutes += 1
                            uptime_hours -= 1
                        continue
                    next_record = one_day_status[i+1]
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
            uptime_days = 7
            downtime_days = 0
            week_data_flag = False
            while total_day > 0:
                for i, record in enumerate(sorted_data):
                    week_data_flag = True
                    time_st = target_time - timedelta(days=total_day-1)
                    if record['timestamp_utc'] >= time_st:
                        limit = i
                        break
                downtime_flag = False
                while itr < limit:
                    sorted_data_len = len(sorted_data)
                    if itr == sorted_data_len:
                        continue
                    if itr+1 >= sorted_data_len:
                        next_status = None
                        next_record = None
                    else:
                        next_record = sorted_data[itr+1]
                        next_status = next_record['status']
                    if itr-1 < 0:
                        previous_status = None
                        previous_time = None
                    else:
                        previous_status = sorted_data[itr - 1]['status']
                        previous_time = sorted_data[itr - 1]['timestamp_utc']
                    next_time = next_record['timestamp_utc']
                    timestamp = sorted_data[itr]['timestamp_utc']
                    status = sorted_data[itr]['status']
                    if timestamp is None:
                        continue
                    day = timestamp.weekday()
                    time_arr = store_timing_dict.get(day)
                    if time_arr is None:
                        continue
                    if store_timing_dict.get(0) == '24/7':
                        start_time = time(0, 0)
                        dt_start = datetime.combine(datetime.today().date(), start_time)
                        end_time = time(23, 59)
                        dt_end = datetime.combine(datetime.today().date(), end_time)
                        time_arr[0] = dt_start
                        time_arr[1] = dt_end
                    start = time_arr[0]
                    end = time_arr[1]
                    if status == 'inactive' and start.time() <= current_timestamp.time() <= end.time():
                        if itr == 0:
                            if sorted_data[itr]['status'] == 'active' and start.time() <= current_timestamp.time() <= end.time():
                                if next_time is not None:
                                    time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                        datetime.min, start.time())
                                    day_count = time_difference.seconds // 86400
                                    downtime_days += day_count
                                    continue
                                else:
                                    downtime_days = 7
                                    continue
                        if itr+1 == sorted_data_len:
                            if sorted_data[itr]['status'] == 'inactive' and start.time() <= current_timestamp.time() <= end.time():
                                if previous_time is not None:
                                    time_difference = datetime.combine(datetime.min, end.time()) - datetime.combine(
                                        datetime.min, sorted_data[itr]['timestamp_utc'].time())
                                    day_count = time_difference.seconds // 86400
                                    downtime_days += day_count
                                    continue
                                else:
                                    downtime_days = 7
                                    continue
                        downtime_flag = True
                        if previous_status is None or next_status is None:
                            downtime_days += 1
                        if previous_status == 'inactive':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, previous_time.time())
                            day_count = time_difference.seconds // 86400
                            downtime_days += day_count
                            continue
                        elif previous_time == 'active':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, timestamp.time())
                            day_count = time_difference.seconds // 86400
                            downtime_days += day_count
                            continue
                    if status == 'active' and start.time() <= current_timestamp.time() <= end.time():
                        if itr == 0:
                            if sorted_data[itr]['status'] == 'active' and start.time() <= current_timestamp.time() <= end.time():
                                if next_time is not None:
                                    time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                        datetime.min, start.time())
                                    day_count = time_difference.seconds // 86400
                                    uptime_days += day_count
                                    continue
                                else:
                                    uptime_days = 7
                                    continue
                        if itr+1 == sorted_data_len:
                            if sorted_data[itr]['status'] == 'inactive' and start.time() <= current_timestamp.time() <= end.time():
                                if previous_time is not None:
                                    time_difference = datetime.combine(datetime.min, end.time()) - datetime.combine(
                                        datetime.min, sorted_data[itr]['timestamp_utc'].time())
                                    day_count = time_difference.seconds // 86400
                                    uptime_days += day_count
                                    continue
                                else:
                                    uptime_days = 7
                                    continue
                        if previous_status is None or next_status is None:
                            uptime_days += 1
                        if previous_status == 'inactive':
                            time_difference = datetime.combine(datetime.min, next_time.time()) - datetime.combine(
                                datetime.min, previous_time.time())
                            day_count = time_difference.seconds // 86400
                            uptime_days += day_count
                            continue
                    itr += 1
                if downtime_flag is False and uptime_days != 7:
                    uptime_days = 7
                total_day -= 1
            if week_data_flag is False:
                uptime_days = -1
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
        generated_report_response[report_id]['status'] = 'Completed'
        generated_report_response[report_id]['data'] = report_data
        db.result.insert_many(report_data)
    except Exception as e:
        generated_report_response[report_id]["status"] = "failed"



def fetch_report(report_id):
    return db.result.find({"report_id": report_id})


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


def calculate_uptime(group, start_time, end_time):
    uptime = group[
        (group['timestamp_utc'] >= start_time) & (group['timestamp_utc'] <= end_time) & (group['status'] == 'active')]
    return uptime['timestamp_utc'].count()


def calculate_downtime(group, start_time, end_time):
    downtime = group[
        (group['timestamp_utc'] >= start_time) & (group['timestamp_utc'] <= end_time) & (group['status'] == 'inactive')]
    return downtime['timestamp_utc'].count()

# Example usage
# csv_files = ['file1.csv', 'file2.csv', 'file3.csv']
# report = generate_report(csv_files, None)
# print(report)
