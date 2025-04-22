{{ config(
    materialized='table',
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT raw_commodity_prices_unique UNIQUE (timestamp, ticker)"
    ]
) }}

select
  cast(null as timestamp) as timestamp,
  cast(null as text) as name,
  cast(null as text) as ticker,
  cast(null as double precision) as price
where false
