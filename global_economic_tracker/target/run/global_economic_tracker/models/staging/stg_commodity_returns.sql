
  
    

  create  table "airflow"."public"."stg_commodity_returns__dbt_tmp"
  
  
    as
  
  (
    

select
    'commodity' as type,
    ticker,
    timestamp,
    price,
    lag(price) over (partition by ticker order by timestamp) as prev_price,
    coalesce((price - lag(price) over (partition by ticker order by timestamp)) / lag(price) over (partition by ticker order by timestamp), 0) as daily_return
from "airflow"."public"."raw_commodity_prices"
  );
  