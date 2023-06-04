# Store-Monitoring

## Problem statement

Loop monitors several restaurants in the US and needs to monitor if the store is online or not. All restaurants are supposed to be online during their business hours. Due to some unknown reasons, a store might go inactive for a few hours. Restaurant owners want to get a report of the how often this happened in the past.   

We want to build backend APIs that will help restaurant owners achieve this goal. 

We will provide the following data sources which contain all the data that is required to achieve this purpose. 

## Data sources

We will have 3 sources of data 

1. We poll every store roughly every hour and have data about whether the store was active or not in a CSV.  The CSV has 3 columns (`store_id, timestamp_utc, status`) where status is active or inactive.  All timestamps are in **UTC**
2. We have the business hours of all the stores - schema of this data is `store_id, dayOfWeek(0=Monday, 6=Sunday), start_time_local, end_time_local`
    1. These times are in the **local time zone**
    2. If data is missing for a store, assume it is open 24*7
3. Timezone for the stores - schema is `store_id, timezone_str`
    1. If data is missing for a store, assume it is America/Chicago
    2. This is used so that data sources 1 and 2 can be compared against each other.

## System requirement

- Do not assume that this data is static and precompute the answers as this data will keep getting updated every hour.
- Need to store these CSVs into a relevant database and make API calls to get the data.

## Data output requirement

Output a report to the user that has the following schema

`store_id, uptime_last_hour(in minutes), uptime_last_day(in hours), update_last_week(in hours), downtime_last_hour(in minutes), downtime_last_day(in hours), downtime_last_week(in hours)` 

1. Uptime and downtime should only include observations within business hours. 
2. Need to extrapolate uptime and downtime based on the periodic polls we have ingested, to the entire time interval.
    1. eg, business hours for a store are 9 AM to 12 PM on Monday
        1. we only have 2 observations for this store on a particular date (Monday) in our data at 10:14 AM and 11:15 AM
        2. we need to fill the entire business hours interval with uptime and downtime from these 2 observations based on some sane interpolation logic

Note: The data we have given is a static data set, so the timestamp is hardcoded based on the store_status data.  

## API requirement

1. You need two APIs 
    1. /trigger_report endpoint that will trigger report generation from the data provided (stored in DB)
        1. No input 
        2. Output - report_id (random string) 
        3. report_id will be used for polling the status of report completion
    2. /get_report endpoint that will return the status of the report or the csv
        1. Input - report_id
        2. Output
            - if report generation is not complete, return “Running” as the output
            - if report generation is complete, return “Complete” along with the CSV file with the schema described above.
            
