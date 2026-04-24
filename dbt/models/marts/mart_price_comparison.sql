-- Same model across different sites — enables cross-site price comparison.
select
    brand,
    model,
    source_site,

    count(*)                as listing_count,
    round(min(price), 2)    as min_price,
    round(max(price), 2)    as max_price,
    round(avg(price), 2)    as avg_price,
    any_value(currency)     as currency,

    max(scraped_date)       as scraped_date,
    current_timestamp()     as updated_at

from {{ ref('int_prices_unified') }}
group by brand, model, source_site
order by brand, model, avg_price
