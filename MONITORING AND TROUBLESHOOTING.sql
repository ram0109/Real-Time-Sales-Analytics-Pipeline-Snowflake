--RESOURCE MONITOR--------------
CREATE OR REPLACE RESOURCE MONITOR RM WITH CREDIT_QUOTA = 1000
TRIGGERS ON 90 PERCENT DO NOTIFY
            ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = RM; 



-- top 5 longest-running queries in the last 24 hours
SELECT query_id,
       user_name,
       warehouse_name,
       execution_status,
       total_elapsed_time AS execution_time_sec,
       start_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY execution_time_sec DESC
LIMIT 5;


-- Task runs in the last 6 hours
SELECT name,
       state,
       error_code,
       error_message,
       scheduled_time,
       completed_time
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE scheduled_time >= DATEADD(hour, -6, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;

-- Snowpipe ingestion report
SELECT 
    PIPE_NAME,
    CREDITS_USED,
    FILES_INSERTED
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY;


-- 

