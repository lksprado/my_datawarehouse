{{
  config(
    tags = ['inflacao','marts'],
    )
}}

with
dim as (
    select * from {{ ref('int_dim_products') }}
),
fct as (
    select * from {{ ref('int_fct_products') }}
),
final as (
    select
        fct.created_at,
        fct.sku,
        dim.product_name,
        dim.category,
        dim.brand_name,
        dim.product_unity,
        dim.unit_normalized,
        dim.quantity_type,
        dim.quantity_value_normalized,
        fct.high_price,
        fct.low_price
    from 
    fct
    inner join dim
        on fct.sku = dim.sku
)
select * from final
order by 
created_at asc,
sku desc
