

select
    'index' as type,
    country,
    ticker,
    timestamp,
    price,
    lag(price) over (partition by ticker order by timestamp) as prev_price,
    coalesce((price - lag(price) over (partition by ticker order by timestamp)) / lag(price) over (partition by ticker order by timestamp), 0) as daily_return
from "airflow"."public"."raw_index_prices"