{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_returns_ticker_ts ON {{ this }}(ticker, timestamp)"
    ]
) }}

select
    'commodity' as type,
    ticker,
    timestamp,
    price,
    lag(price) over (partition by ticker order by timestamp) as prev_price,
    coalesce((price - lag(price) over (partition by ticker order by timestamp)) / lag(price) over (partition by ticker order by timestamp), 0) as daily_return
from public.commodity_prices
