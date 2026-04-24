{{
    config(materialized='ephemeral')
}}

with jumia as (
    select * from {{ ref('stg_jumia') }}
),

electroplanet as (
    select * from {{ ref('stg_electroplanet') }}
),

amazon as (
    select * from {{ ref('stg_amazon') }}
),

unified as (
    select * from jumia
    union all
    select * from electroplanet
    union all
    select * from amazon
)

select * from unified
