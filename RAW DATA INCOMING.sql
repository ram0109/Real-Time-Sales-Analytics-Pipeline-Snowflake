CREATE OR REPLACE DATABASE SALES_ANALYTICS_DB;
CREATE OR REPLACE SCHEMA RAW;


CREATE OR REaPLACE FILE FORMAT FF
STRIP_OUTER_ARRAY = TRUE
TYPE='json';

CREATE OR REPLACE STAGE INT_STAGE
FILE_FORMAT= FF;


LIST @SALES_ANALYTICS_DB.RAW.INT_STAGE;

CREATE OR REPLACE TABLE SALES_ANALYTICS_DB.RAW.TEMP (
v VARIANT
);

CREATE OR REPLACE PIPE SNOW
AUTO_INGEST = FALSE
AS
COPY INTO SALES_ANALYTICS_DB.RAW.TEMP
FROM @SALES_ANALYTICS_DB.RAW.INT_STAGE;

-- SELECT SYSTEM$PIPE_STATUS('TEST.SC.SNOW');
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'TEMP', START_TIME=>DATEADD(hours, -1, CURRENT_TIMESTAMP())));

CREATE OR REPLACE TASK pipe_refresh_task
SCHEDULE = '1 MINUTE'
AS
ALTER PIPE SALES_ANALYTICS_DB.RAW.SNOW REFRESH;

SELECT * FROM SALES_ANALYTICS_DB.RAW.TEMP;



CREATE OR REPLACE  TABLE SALES_STAGING (    
    order_id STRING,
    product_id STRING,
    product_name STRING,
    category string,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    region STRING,
    order_timestamp TIMESTAMP_NTZ
);

create or replace stream st on table SALES_ANALYTICS_DB.RAW.temp
append_only = true;


CREATE OR REPLACE TASK stage_loading_task
AFTER pipe_refresh_task
AS
INSERT INTO SALES_STAGING (ORDER_ID, PRODUCT_ID, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, REGION, ORDER_TIMESTAMP)
SELECT v:order_id, v:product_id, v:product_name, v:category, v:quantity, v:unit_price, v:total_amount, v:region, v:order_timestamp
from st;

ALTER TASK pipe_refresh_task SUSPEND;
ALTER TASK pipe_refresh_task RESUME;
ALTER TASK stage_loading_task RESUME;
ALTER TASK stage_loading_task SUSPEND;
ALTER TASK analytics_task SUSPEND;
ALTER TASK analytics_task RESUME;



-- create or replace stream st on test.sc.temp
-- append_only = true;


select * from st;
select * from sales_staging where order_id='ord-43156';


show tasks;

-- tk - pipe_refresh_task
-- tk1 - stage_loading_task
-- tk2 - analytics_task






