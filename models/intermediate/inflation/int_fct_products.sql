{{
  config(
    materialized = 'view',
    tags = ['inflacao', 'intermediate'],
  )
}}

with 
source as (
    select * from {{ ref('stg_atacadao') }}
),
final as (
    select 
    distinct
    created_at,
    sku,
    high_price,
    low_price
    from source
)
select * from final 
