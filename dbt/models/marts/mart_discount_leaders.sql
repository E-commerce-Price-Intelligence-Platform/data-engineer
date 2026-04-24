-- Top discounted products — highest computed saving vs. original price.
select
    source_site,
    brand,
    model,
    name,

    price,
    old_price,
    currency,
    discount                                                    as raw_discount_label,

    round(old_price - price, 2)                                 as saving_amount,
    round(safe_divide(old_price - price, old_price) * 100, 1)  as discount_pct,

    scraped_date,
    url

from {{ ref('int_prices_unified') }}
where old_price is not null
  and old_price > price
order by discount_pct desc
limit 100
