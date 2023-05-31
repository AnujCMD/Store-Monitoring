import pandas as pd
from datetime import datetime, timedelta

# Define the business hours for a store (9 AM to 12 PM on Monday)
business_hours_start = datetime.strptime('09:00:00', '%H:%M:%S').time()
business_hours_end = datetime.strptime('12:00:00', '%H:%M:%S').time()


def generate_report(csv_file_list):
    # Load and merge all the CSV files into a single DataFrame
    df = pd.concat([pd.read_csv(file) for file in csv_file_list])

    # Convert timestamp_utc column to datetime
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])

    # Filter data within business hours
    df = df[(df['timestamp_utc'].dt.time >= business_hours_start) & (df['timestamp_utc'].dt.time <= business_hours_end)]

    # Compute the current timestamp as the maximum timestamp among all observations
    current_timestamp = df['timestamp_utc'].max()

    # Group data by store_id and calculate uptime and downtime metrics
    grouped = df.groupby('store_id')
    report_data = []

    for store_id, group in grouped:
        uptime_last_hour = calculate_uptime(group, current_timestamp - timedelta(hours=1), current_timestamp)
        uptime_last_day = calculate_uptime(group, current_timestamp - timedelta(days=1), current_timestamp)
        uptime_last_week = calculate_uptime(group, current_timestamp - timedelta(weeks=1), current_timestamp)

        downtime_last_hour = calculate_downtime(group, current_timestamp - timedelta(hours=1), current_timestamp)
        downtime_last_day = calculate_downtime(group, current_timestamp - timedelta(days=1), current_timestamp)
        downtime_last_week = calculate_downtime(group, current_timestamp - timedelta(weeks=1), current_timestamp)

        report_data.append({
            'store_id': store_id,
            'uptime_last_hour': uptime_last_hour,
            'uptime_last_day': uptime_last_day,
            'uptime_last_week': uptime_last_week,
            'downtime_last_hour': downtime_last_hour,
            'downtime_last_day': downtime_last_day,
            'downtime_last_week': downtime_last_week
        })

    return report_data


def calculate_uptime(group, start_time, end_time):
    uptime = group[
        (group['timestamp_utc'] >= start_time) & (group['timestamp_utc'] <= end_time) & (group['status'] == 'active')]
    return uptime['timestamp_utc'].count()


def calculate_downtime(group, start_time, end_time):
    downtime = group[
        (group['timestamp_utc'] >= start_time) & (group['timestamp_utc'] <= end_time) & (group['status'] == 'inactive')]
    return downtime['timestamp_utc'].count()


# Example usage
csv_files = ['file1.csv', 'file2.csv', 'file3.csv']
report = generate_report(csv_files)
print(report)
