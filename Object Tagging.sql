use database sales_analytics_db;

create or replace schema tag_schema;

create or replace tag revenue_tag comment='revenue info is masked';
create or replace role developer;
-- Create masking policy
CREATE OR REPLACE MASKING POLICY revenue_mask 
  AS (val DECIMAL) 
  RETURNS DECIMAL ->
    CASE 
      WHEN CURRENT_ROLE() in ( 'DEVELOPER') THEN NULL
      ELSE VAL
    END;

select current_role();
show roles;
-- Attach policy to the tag 
ALTER TAG revenue_tag 
SET MASKING POLICY revenue_mask;

  use role finance_team;
  grant role finance_team to user WARDOX;
use role ACCOUNTADMIN;
  create or replace role rl_analytics_dashboard;
use role rl_analytics_dashboard;
  use role SECURITYADMIN;
  grant select, update on table analytics.fact_sales to role rl_analytics_dashboard;
  
  grant role rl_analytics_dashboard to user WARDOX;
-- Now whenever a column is tagged, masking will apply automatically
ALTER TABLE analytics.fact_sales 
MODIFY COLUMN total_amount 
SET TAG revenue_tag='revenue info is masked';

select * from analytics.fact_sales;

grant select on  all views in schema SALES_ANALYTICS_DB.ANALYTICS to role rl_analytics_dashboard;
grant select on all materialized views in database SALES_ANALYTICS_DB to role rl_analytics_dashboard;

SELECT * FROM ANALYTICS.SPM;
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY

LIMIT 10;


grant role developer to user WARDOX;

use role developer;

create or replace user developer_user
password = 'Tredence@1234';

grant role developer to user developer_user;

grant usage on database sales_analytics_db to role developer;
grant usage on schema analytics to role developer;

grant select on table analytics.fact_sales to role developer;
grant usage on  warehouse compute_wh to role developer;
select * from analytics.fact_sales;


show tags;

  


