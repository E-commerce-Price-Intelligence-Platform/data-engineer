select
    brand,

    count(*)                                    as product_count,
    count(distinct model)                       as unique_models,
    count(distinct source_site)                 as sites_present,

    round(min(price), 2)                        as min_price,
    round(max(price), 2)                        as max_price,
    round(avg(price), 2)                        as avg_price,
    round(stddev(price), 2)                     as price_stddev,

    countif(old_price is not null
            and old_price > price)              as discounted_count,
    round(
        safe_divide(
            countif(old_price is not null and old_price > price),
            count(*)
        ) * 100, 1
    )                                           as discount_rate_pct,

    max(scraped_date)                           as last_scraped_date,
    current_timestamp()                         as updated_at

from {{ ref('int_prices_unified') }}
group by brand
order by product_count desc
