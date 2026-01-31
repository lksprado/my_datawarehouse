{{
  config(
    materialized = 'table',
    tags = ['inflacao', 'staging'],
  )
}}

with 
source as (
    select * from {{ source('raw', 'atacadao_raw')}}
),
renamed as (
    select
    TO_DATE(extracted_at, 'YYYY-MM-DD') as created_at,
    sku,
    category,
    product_name,
    case
        -- CASO 1: existe número + unidade → pega a última ocorrência
        when regexp_count(
            lower(product_name),
            '(\d+([.,]\d+)?\s*(mg|g|kg|ml|l|un|rolos|quilo|dúzias|dúzia|folhas))'
        ) > 0
        then regexp_substr(
            lower(product_name),
            '(\d+([.,]\d+)?\s*(mg|g|kg|ml|l|un|rolos|quilo|dúzias|dúzia|folhas))',
            1,
            regexp_count(
                lower(product_name),
                '(\d+([.,]\d+)?\s*(mg|g|kg|ml|l|un|rolos|quilo|dúzias|dúzia|folhas))'
            )
        )

        -- CASO 2: não tem número → unidade explícita (assume 1)
        when lower(product_name) ~* '(^|\s)(un)(\s|$)'
            then 'un'
        when lower(product_name) ~* '(^|\s)(maço)(\s|$)'
            then 'un'
        when lower(product_name) ~* '(^|\s)(quilo)(\s|$)'
            then 'quilo'

        else null
    end as product_unity,
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
        when product_unity ~* '(litro|l|l\\b|ml)' and product_unity not like '%folhas%' then 'volume'
        -- UNIDADE / EMBALAGEM CONTÁVEL
        when product_unity ~* '(un|folhas|dúzias|dúzia)' then 'unit'
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