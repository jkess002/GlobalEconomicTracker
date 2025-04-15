

with base as (
    select
        *,
        row_number() over (partition by ticker order by timestamp) as rn
    from "airflow"."public"."stg_commodity_returns"
),

rolling as (
    select
        ticker,
        timestamp,
        avg(daily_return) over (
            partition by ticker
            order by rn
            rows between 19 preceding and current row
        ) as rolling_avg_mo,

        avg(daily_return) OVER (
            partition by ticker
            order by timestamp
            rows between 6 preceding and current row
        ) as weekly_avg_return,

        stddev(daily_return) over (
            partition by ticker
            order by rn
            rows between 19 preceding and current row
        ) as rolling_volatility_mo,

        case
            when daily_return > 0 then 'up'
            when daily_return < 0 then 'down'
            else 'flat'
        end as trend,

        daily_return

    from base
)

select
    *,
    rolling_avg_mo / nullif(rolling_volatility_mo, 0) as sharpe_mo,
    (daily_return - rolling_avg_mo) / nullif(rolling_volatility_mo, 0) as z_score_mo
from rolling
where rolling_avg_mo is not null