{{
  config(
    tags = ['livros','marts'],
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
    t1.created_at,
    t1.sku,
    t2.product_name,
    t2.category,
    t2.brand_name,
    t2.product_unity,
    t2.unit_normalized,
    t2.quantity_type,
    t2.quantity_value_normalized,
    t1.high_price,
    t1.low_price
    from 
    fct t1 
    inner join dim t2
    on t1.sku = t2.sku
)
select * from final
order by 
created_at asc,
sku desc
