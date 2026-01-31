{{
  config(
    materialized = 'table',
    tags = ['inflacao', 'staging'],
  )
}}

with 
source as (
    select * from {{ source('raw', 'atacadao_historico')}}
),
renamed as (
    select
    date_scrapped as created_at,
    sku,
    category,
    product_name,
    lower(CASE
        WHEN regexp_count(product_name, ' com ') = 1
            THEN split_part(product_name, ' com ', 2)

        WHEN regexp_count(product_name, ' com ') >= 2
            THEN split_part(product_name, ' com ', 3)

        ELSE NULL
    END) AS product_unity,
    brand_name,
    high_price,
    low_price
    from source
),
units as (
    select
    *,
    case
        -- PESO
        when product_unity ~* '(kg|quilo|g|grama|g\\b)' then 'weight'
        -- VOLUME
        when product_unity ~* '(litro|l|l\\b|ml)' then 'volume'
        -- UNIDADE / EMBALAGEM CONTÁVEL
        when product_unity ~* '(un)' then 'unit'
        else 'unknown'
    end as quantity_type,
        -- VALOR NUMÉRICO BRUTO
    case
        when product_unity ~* '^(kg|quilo|litro|l|ml)$' then 1::numeric

        when product_unity ~* '[0-9]' then
            replace(
                regexp_replace(product_unity, '[^0-9.,]', '', 'g'),
                ',',
                '.'
            )::numeric

        else 1
    end as quantity_value_raw
    from renamed
    where lower(category) in ('bebidas','carnes, aves e peixes','frios e congelados', 'hortifrúti', 'limpeza', 'mercearia', 'padaria e matinais')
)
select * from units