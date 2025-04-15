

with base as (
    select
        timestamp,
        ticker,
        type,
        price,
        row_number() over (partition by ticker order by timestamp) as rn
    from raw_prices
),

returns as (
    select
        b1.timestamp,
        b1.ticker,
        b1.type,
        b1.price,
        (b1.price - b0.price) / nullif(b0.price,  0) as daily_return
    from base b1
    left join base b0
        on b1.ticker = b0.ticker
        and b1.rn = b0.rn + 1
)

select * from returns