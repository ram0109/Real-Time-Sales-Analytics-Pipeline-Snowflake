create or replace schema clone_schema;

create or replace table  fact_sales_clone clone analytics.fact_sales;
create or replace table dim_product_clone clone analytics.dim_product;

grant usage on schema clone_schema to role marketing_team;
grant all privileges on table fact_sales_clone to role marketing_team;
grant all privileges on table dim_product_clone to role marketing_team;


CREATE or replace TABLE FACT_SALES_YESTERDAY CLONE SALES_ANALYTICS_DB.ANALYTICS.FACT_SALES
AT (OFFSET => -86400);

CREATE or replace TABLE dim_product_YESTERDAY CLONE SALES_ANALYTICS_DB.ANALYTICS.DIM_PRODUCT
AT (OFFSET => -86400);

grant usage on schema clone_schema to role marketing_team;
grant all privileges on table FACT_SALES_YESTERDAY to role marketing_team;
grant all privileges on table dim_product_yesterday to role marketing_team;



CREATE OR REPLACE TABLE SALES_FACT_RESTORE AS
SELECT *
FROM SALES_ANALYTICS_DB.ANALYTICS.FACT_SALES
AT (TIMESTAMP => TO_TIMESTAMP('2025-09-08 12:00:00'));

select * from sales_fact_restore;


delete from sales_analytics_db.clone_schema.sales_fact_restore
where sale_date = '2025-09-08';


select * from sales_analytics_db.clone_schema.sales_fact_restore;

select * from sales_analytics_db.clone_schema.sales_fact_restore
be

