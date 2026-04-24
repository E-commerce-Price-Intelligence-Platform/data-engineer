-- Price velocity: day-over-day change per (site, brand, model).
-- Requires at least 2 daily snapshots to produce rows.
with daily as (
    select * from {{ ref('mart_daily_prices') }}
),

with_lag as (
    select
        price_day_sk,
        source_site,
        brand,
        model,
        scraped_date,
        avg_price,
        currency,

        lag(avg_price) over (
            partition by source_site, brand, model
            order by scraped_date
        ) as prev_avg_price,

        lag(scraped_date) over (
            partition by source_site, brand, model
            order by scraped_date
        ) as prev_scraped_date

    from daily
),

velocity as (
    select
        price_day_sk,
        source_site,
        brand,
        model,
        scraped_date,
        prev_scraped_date,
        avg_price,
        prev_avg_price,
        currency,

        round(avg_price - prev_avg_price, 2)                              as price_change,
        round(safe_divide(avg_price - prev_avg_price, prev_avg_price) * 100, 2)
                                                                          as price_change_pct,

        case
            when avg_price < prev_avg_price then 'decrease'
            when avg_price > prev_avg_price then 'increase'
            else 'stable'
        end                                                               as price_trend,

        date_diff(scraped_date, prev_scraped_date, day)                   as days_since_prev

    from with_lag
    where prev_avg_price is not null
)

select * from velocity
