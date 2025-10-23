
USE DATABASE sales_analytics_db;
CREATE OR REPLACE SCHEMA ANALYTICS;

create or replace stream gold_stream on TABLE SALES_ANALYTICS_DB.RAW.sales_staging;
create or replace stream prod_stream on TABLE SALES_ANALYTICS_DB.RAW.sales_staging;

--creating fact sales table
CREATE OR REPLACE TABLE SALES_ANALYTICS_DB.ANALYTICS.FACT_SALES (
    sale_id int autoincrement(1,1),
    order_id STRING,
    product_id STRING,
    region STRING,
    quantity INTEGER,
    total_amount DECIMAL(10,2),
    sale_date DATE,
    sale_timestamp TIMESTAMP_NTZ
) CLUSTER BY (sale_date, region);

--creating dim product table
CREATE or replace TABLE SALES_ANALYTICS_DB.ANALYTICS.DIM_PRODUCT (
    product_id STRING PRIMARY KEY,
    product_name STRING,
    category STRING,
    unit_price DECIMAL(10,2)
);

create or replace table SALES_ANALYTICS_DB.ANALYTICS.dim_calendar
(
    date_key date primary key,
    year int,
    Q int,
    day int,
    day_name string,
    month int,
    month_name string,
    is_weekend boolean    
);



INSERT INTO dim_calendar (
    date_key,
    year,
    q,
    day,
    day_name,
    month,
    month_name,
    is_weekend
)
WITH date_range AS (
    SELECT DATE('2025-01-01') AS date_val
    UNION ALL
    SELECT DATEADD(DAY, 1, date_val)
    FROM date_range
    WHERE date_val < DATE('2027-12-31')
)
SELECT
    date_val,
    EXTRACT(YEAR FROM date_val) AS year,
    CEIL(EXTRACT(MONTH FROM date_val) / 3) AS q,   -- Snowflake has no QUARTER() function
    EXTRACT(DAY FROM date_val) AS day,
    DAYNAME(date_val) AS day_name,
    EXTRACT(MONTH FROM date_val) AS month,
    MONTHNAME(date_val) AS month_name,
    CASE WHEN DAYOFWEEK(date_val) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_range;

--creating stored procedure
CREATE OR REPLACE PROCEDURE SALES_ANALYTICS_DB.ANALYTICS.PROCESS_NEW_SALES()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN
    INSERT INTO SALES_ANALYTICS_DB.ANALYTICS.FACT_SALES(order_Id,product_id,region,quantity,total_amount,sale_date,sale_timestamp)
    SELECT 
        order_id,
        product_id,
        region,
        quantity,
        total_amount,
        DATE(order_timestamp),
        order_timestamp
    FROM SALES_ANALYTICS_DB.ANALYTICS.GOLD_STREAM;

    -----------------------------------------------------
    MERGE INTO SALES_ANALYTICS_DB.ANALYTICS.DIM_PRODUCT t
    USING (
        SELECT product_id, product_name,category, unit_price,METADATA$ACTION, METADATA$ISUPDATE
        from SALES_ANALYTICS_DB.ANALYTICS.prod_stream
    ) s
    on s.product_id = t.product_id
    
    when matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = TRUE
    then update 
    set
    t.product_id = s.product_id,
    t.product_name = s.product_name,
    t.category = s.category,
    t.unit_price = s.unit_price
    
    when not matched
    then insert (product_id,product_name,category,unit_price)
    values (s.product_id,s.product_name,s.category,s.unit_price);
    RETURN 'Processed new sales data';
END;

--creating task to call stored procedure
CREATE OR REPLACE TASK SALES_ANALYTICS_DB.RAW.analytics_task
AFTER SALES_ANALYTICS_DB.RAW.stage_loading_task
AS
CALL SALES_ANALYTICS_DB.ANALYTICS.PROCESS_NEW_SALES();


SELECT * FROM GOLD_STREAM;
select * from prod_stream;
SELECT * FROM FACT_SALES where order_id = 'ORD-43116';
SELECT * FROM DIM_PRODUCT;
select * from dim_calendar;




--MATERIALIZED VIEW FOR SALES PER MINUTE

CREATE OR REPLACE VIEW SALES_ANALYTICS_DB.ANALYTICS.SPM
AS
SELECT 
REGION,
DATE_TRUNC('minute',sale_timestamp) as mins,
count(sale_id) as tot_sales
from fact_sales
group by REGION,mins;

DROP MATERIALIZED VIEW SPM;
--MATERIALIZED VIEW SALES BY REGION
DROP MATERIALIZED VIEW SALES_ANALYTICS_DB.ANALYTICS.SPR;
CREATE OR REPLACE VIEW SALES_ANALYTICS_DB.ANALYTICS.SPR
AS 
select region,
SUM(total_amount) as Revenue,
COUNT(SALE_ID) AS NO_OF_SALES,
SUM(QUANTITY) AS TOT_QUANTITY,
COUNT(DISTINCT ORDER_ID) AS TOT_ORDERS,
ROUND(SUM(TOTAL_AMOUNT)/COUNT(DISTINCT ORDER_ID),2) AS AVERAGE_ORDER_VALUE
from fact_sales 
group by region;

--MATERIALIZED VIEW TOP PRODUCTS
-- CREATE OR REPLACE TABLE TOP_PROD
-- (
--     PRODUCT_NAME STRING,
--     CATEGORY STRING,
--     REVENUE FLOAT
-- );


-- CREATE OR REPLACE TASK TK3
-- AFTER TK2
-- AS
-- INSERT INTO TOP_PROD (PRODUCT_NAME,CATEGORY, REVENUE)
-- SELECT
-- P.PRODUCT_NAME,
-- P.CATEGORY,
-- SUM(TOTAL_AMOUNT) AS PRODUCT_REVENUE
-- FROM
-- FACT_SALES F JOIN DIM_PRODUCT P
-- ON F.PRODUCT_ID = P.PRODUCT_ID
-- GROUP BY PRODUCT_NAME,CATEGORY
-- ORDER BY SUM(TOTAL_AMOUNT) DESC;



-- CREATE OR REPLACE MATERIALIZED VIEW TP
-- AS
-- SELECT
-- PRODUCT_NAME,
-- REVENUE
-- FROM TOP_PROD;



-- MATERIALIZED VIEW TOP CATEGORY
-- CREATE OR REPLACE MATERIALIZED VIEW TC
-- AS
-- SELECT 
-- CATEGORY,
-- SUM(REVENUE) AS TOT_REVENUE_BY_CATEGORY
-- FROM TOP_PROD
-- GROUP BY CATEGORY;

-- ALTER TASK TK3 RESUME;

-- VIEW FOR TOP_PRODUCTS
CREATE OR REPLACE VIEW SALES_ANALYTICS_DB.ANALYTICS.TOP_PRODUCTS
AS
SELECT 
P.PRODUCT_NAME,
SUM(TOTAL_AMOUNT) AS REVENUE,
REGION
FROM FACT_SALES F JOIN DIM_PRODUCT P
ON P.PRODUCT_ID = F.PRODUCT_ID
GROUP BY REGION,P.PRODUCT_NAME
ORDER BY SUM(TOTAL_AMOUNT) DESC;


--VIEW FOR TOP_CATEGORY
CREATE OR REPLACE VIEW SALES_ANALYTICS_DB.ANALYTICS.TOP_CATEGORY
AS
SELECT 
P.CATEGORY,
SUM(TOTAL_AMOUNT) AS REVENUE,
REGION
FROM FACT_SALES F JOIN DIM_PRODUCT P
ON P.PRODUCT_ID = F.PRODUCT_ID
GROUP BY REGION,P.CATEGORY
ORDER BY SUM(TOTAL_AMOUNT) DESC;


-- DROP MATERIALIZED VIEW TP;
-- DROP MATERIALIZED VIEW TC;
-- DROP TABLE TOP_PROD;

select * from spm;
SELECT * FROM SPR;
SELECT * FROM TOP_PRODUCTS;
SELECT * FROM TOP_CATEGORY;


--KPI'S-----------------------------------------
--TOT REVENUE
-- TOT UNITS SOLD
-- TOT ORDERS
-- AVERAGE ORDER VALUE

CREATE OR REPLACE MATERIALIZED VIEW SALES_ANALYTICS_DB.ANALYTICS.TOT_REVENUE
AS
SELECT
SUM(TOTAL_AMOUNT) AS TOTAL_REVENUE,
REGION
FROM FACT_SALES
GROUP BY REGION;


SELECT * FROM TOT_REVENUE;

--materalized view for total unit sold
CREATE OR REPLACE MATERIALIZED VIEW SALES_ANALYTICS_DB.ANALYTICS.TOT_UNITS
AS
SELECT
SUM(QUANTITY) AS TOTAL_UNITS
FROM FACT_SALES;

SELECT * FROM TOT_UNITS;


CREATE OR REPLACE VIEW SALES_ANALYTICS_DB.ANALYTICS.TOT_ORDERS
AS
SELECT 
COUNT(DISTINCT ORDER_ID) AS TOTAL_ORDERS
FROM FACT_SALES;

SELECT * FROM TOT_ORDERS;

CREATE OR REPLACE VIEW SALES_ANALYTICS_DB.ANALYTICS.AVERAGE_VALUE
AS
SELECT
ROUND(1.0*SUM(TOTAL_AMOUNT)/COUNT(DISTINCT ORDER_ID),2) AS AVERAGE_ORDER_VALUE
FROM FACT_SALES;

SELECT * FROM AVERAGE_VALUE;


CREATE OR REPLACE VIEW TOP_PRODUCTS_PER_REGION
AS
WITH CTE AS(
SELECT 
F.REGION,
P.PRODUCT_NAME,
SUM(F.TOTAL_AMOUNT) AS REVENUE_OF_PRODUCT
FROM 
FACT_SALES F JOIN DIM_PRODUCT P ON F.PRODUCT_ID = P.PRODUCT_ID
GROUP BY F.REGION, P.PRODUCT_NAME
),
CTE1 AS(
SELECT
REGION,
PRODUCT_NAME,
RANK() OVER (PARTITION BY REGION ORDER BY REVENUE_OF_PRODUCT DESC) AS RANK,
revenue_of_product
FROM CTE
)
SELECT
REGION,
PRODUCT_NAME,
revenue_of_product
FROM CTE1 
WHERE RANK<=3;


SELECT * FROM TOP_PRODUCTS_PER_REGION;
SHOW TASKS;





