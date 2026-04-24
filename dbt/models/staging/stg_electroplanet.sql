with source as (
    select * from {{ source('raw_prices', 'electroplanet') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['name', 'price', 'scraped_at', 'source_site']) }}
            as product_sk,

        trim(name)                              as name,
        lower(trim(coalesce(brand, 'unknown'))) as brand,
        lower(trim(coalesce(model, 'unknown'))) as model,

        cast(price as float64)                  as price,
        cast(old_price as float64)              as old_price,
        coalesce(currency, 'DH')                as currency,
        discount,

        cast(rating as float64)                 as rating,
        cast(reviews as int64)                  as reviews,
        url,
        source_site,

        timestamp(scraped_at)                   as scraped_at,
        date(scraped_at)                        as scraped_date

    from source
    where price is not null
      and cast(price as float64) > 0
)

select * from cleaned
