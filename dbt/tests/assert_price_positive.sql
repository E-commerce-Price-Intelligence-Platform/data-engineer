-- Fails if any unified record carries a non-positive price.
select
    product_sk,
    source_site,
    name,
    price
from {{ ref('int_prices_unified') }}
where price <= 0
