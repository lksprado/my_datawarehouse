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
reduced as (
    select 
    distinct 
    sku,
    lower(product_name) as product_name,
    length(product_name) as num_name,
    category,
    product_unity,
    unit_normalized,
    brand_name,
    quantity_type,
    quantity_value_raw,
    quantity_value_normalized
    from source 
),
rank as (
    select 
    row_number() over (partition by sku order by num_name desc) as rn,
    sku,
    trim(
        regexp_replace(
            lower(product_name),
            '\s*\d+(\.\d+)?\s*(mg|g|kg|ml|l|un)$',
            ''
        )
    ) as product_name,
    category,
    product_unity,
    unit_normalized,
    brand_name,
    quantity_type,
    quantity_value_raw,
    quantity_value_normalized
    from reduced
),
final as (
    select 
    sku,
    replace(trim(
        regexp_replace(
            product_name,
            '\s+com$',
            ''
        )
    ),',','') AS product_name,
    category,
    brand_name,
    product_unity,
    unit_normalized,
    quantity_type,
    quantity_value_raw,
    round(quantity_value_normalized, 3) as quantity_value_normalized
    from rank
    where rn = 1
)
select * from final 
