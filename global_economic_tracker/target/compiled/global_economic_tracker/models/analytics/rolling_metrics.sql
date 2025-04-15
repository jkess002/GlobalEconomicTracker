

with base as (
    select
        *,
        row_number() over (partition by ticker order by timestamp) as rn
    from "airflow"."public"."daily_returns"
),

rolling as (
    select
        ticker,
        timestamp,
        daily_return,

        avg(daily_return) over (
            partition by ticker
            order by rn
            rows between 19 preceding and current row
        ) as rolling_avg_mo,

        stddev(daily_return) over (
            partition by ticker
            order by rn
            rows between 19 preceding and current row
        ) as rolling_volatility_mo
    from base
),

final as (
    select
        *,
        (rolling_avg_mo / nullif(rolling_volatility_mo, 0)) as sharpe_mo,
        (daily_return - rolling_avg_mo) / nullif(rolling_volatility_mo, 0) as z_score_mo
    from rolling
)

select *
from final
where rolling_volatility_mo is not null