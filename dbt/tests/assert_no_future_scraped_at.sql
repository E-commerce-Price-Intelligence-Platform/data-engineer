-- Fails if scraped_at is in the future — indicates a clock or parsing error.
select
    product_sk,
    source_site,
    name,
    scraped_at
from {{ ref('int_prices_unified') }}
where scraped_at > current_timestamp()
