-- Warns if any model's price changed by more than 50 % in a single day.
-- Expected to trigger on first run (no prior day to compare against).
{{ config(severity='warn') }}
select
    price_day_sk,
    source_site,
    brand,
    model,
    scraped_date,
    prev_scraped_date,
    avg_price,
    prev_avg_price,
    price_change_pct
from {{ ref('mart_price_velocity') }}
where abs(price_change_pct) > 50
