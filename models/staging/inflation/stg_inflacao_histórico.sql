{{
  config(
    materialized = 'table',
    tags = ['inflacao', 'staging'],
  )
}}

with 
source as (
    select * from {{ source('raw', 'minha_inflacao') }}
),
units as (
    select * from {{ ref('stg_eph_quantidade') }}
),
renamed as (
    select 
        regexp_replace(public.unaccent(lower("Produto")), '[^a-z0-9]', '', 'g') clean_product_name,
        "Produto" as product_name,
        "Unidade" as product_unity,
        "Qtd" as quantity,
        "Preço" as price,
        "Mês Atual" as current_month,
        "Peso" as weight,
        "Mês passado" as past_month,
        "Var" as price_variation,
        "Var Peso" as weight_variation,
        "Categoria" as category,
        "Mes" as month,
        "Ano" as year
    from source 
),
final as (
    select 
    (
        make_date(t1.year, t1.month, 1)
        + interval '1 month'
        - interval '1 day'
    )::date as created_at,
    {{ dbt_utils.generate_surrogate_key(['t1.clean_product_name']) }} as sk_product,
    t1.category,
    t1.product_name,
    t1.product_unity,
    t2.quantity_type,
    t2.quantity_value_raw,
    t2.quantity_value_std,
    t1.quantity,
    t1.price,
    t1.current_month,
    t1.past_month,
    t1.price_variation,
    t1.weight,
    t1.weight_variation
    from renamed t1
    left join units t2 
    on {{ dbt_utils.generate_surrogate_key(['t1.clean_product_name']) }} = t2.sk_product
)
select * from final 
