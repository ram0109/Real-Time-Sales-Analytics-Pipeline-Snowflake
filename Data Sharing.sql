USE DATABASE SALES_ANALYTICS_DB;

CREATE OR REPLACE SCHEMA ROLES_SCHEMA;
create or replace role marketing_team;
create or replace role finance_team;
create or replace role executive;

create or replace user marketing_user
password='Tredence@1234';

create or replace user finance_user
password='Tredence@1234';


grant role marketing_team to user marketing_user;
grant role finance_team to user finance_user;
create or replace share marketing_share;


grant usage on database SALES_ANALYTICS_DB to share marketing_share;
grant usage on schema SALES_ANALYTICS_DB.ANALYTICS to share marketing_share;
grant select on table SALES_ANALYTICS_DB.ANALYTICS.fact_sales to share marketing_share;

alter share marketing_share add account =ZF12823;

ALTER SHARE marketing_share 
ADD ORGANIZATION = KGUFIYY;


grant usage on database SALES_ANALYTICS_DB to role marketing_team;
grant usage on schema SALES_ANALYTICS_DB.ANALYTICS to role marketing_team;
grant select on table SALES_ANALYTICS_DB.ANALYTICS.fact_sales to role marketing_team;

grant usage on database SALES_ANALYTICS_DB to role finance_team;
grant usage on schema SALES_ANALYTICS_DB.ANALYTICS to role finance_team;
grant select on view SALES_ANALYTICS_DB.ANALYTICS.top_products to role finance_team;
grant select on materialized view SALES_ANALYTICS_DB.ANALYTICS.spr to role finance_team;

grant usage on database SALES_ANALYTICS_DB to role executive;
grant usage on schema SALES_ANALYTICS_DB.ANALYTICS to role executive;
grant select on  all views in schema SALES_ANALYTICS_DB.ANALYTICS to role executive;
grant select on all materialized views in database SALES_ANALYTICS_DB to role executive;
grant select on  all tables in schema SALES_ANALYTICS_DB.ANALYTICS to role executive;





