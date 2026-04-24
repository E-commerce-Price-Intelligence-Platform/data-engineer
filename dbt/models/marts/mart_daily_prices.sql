{{
    config(
        materialized='incremental',
        unique_key='price_day_sk',
        incremental_strategy='merge',
        partition_by={
            "field": "scraped_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["brand", "source_site"]
    )
}}

with base as (
    select * from {{ ref('int_prices_unified') }}

    {% if is_incremental() %}
        -- On incremental runs, re-process the latest date to capture same-day re-runs
        where scraped_date >= (select max(scraped_date) from {{ this }})
    {% endif %}
),

aggregated as (
    select
        {{ dbt_utils.generate_surrogate_key(['source_site', 'brand', 'model', 'scraped_date']) }}
            as price_day_sk,

        source_site,
        brand,
        model,
        scraped_date,

        round(avg(price), 2)    as avg_price,
        min(price)              as min_price,
        max(price)              as max_price,
        count(*)                as listing_count,
        any_value(currency)     as currency,

        current_timestamp()     as updated_at

    from base
    group by source_site, brand, model, scraped_date
)

select * from aggregated
