{{
  config(
    materialized = 'ephemeral',
    tags = ['inflacao', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw', 'atacadao_historico') }}
),

renamed AS (
    SELECT
        date_scrapped AS created_at,
        sku,
        category,
        product_name,
        brand_name,
        high_price,
        low_price,
        LOWER(CASE
            WHEN REGEXP_COUNT(product_name, ' com ') = 1
                THEN SPLIT_PART(product_name, ' com ', 2)

            WHEN REGEXP_COUNT(product_name, ' com ') >= 2
                THEN SPLIT_PART(product_name, ' com ', 3)
        END)          AS product_unity
    FROM source
),

units AS (
    SELECT
        *,
        CASE
        -- PESO
            WHEN product_unity ~* '(kg|quilo|g|grama|g\\b)' THEN 'weight'
            -- VOLUME
            WHEN product_unity ~* '(litro|l|l\\b|ml)' THEN 'volume'
            -- UNIDADE / EMBALAGEM CONTÁVEL
            WHEN product_unity ~* '(un)' THEN 'unit'
            ELSE 'unknown'
        END AS quantity_type,
        -- VALOR NUMÉRICO BRUTO
        CASE
            WHEN product_unity ~* '^(kg|quilo|litro|l|ml)$' THEN 1::NUMERIC

            WHEN product_unity ~* '[0-9]'
                THEN
                    REPLACE(
                        REGEXP_REPLACE(product_unity, '[^0-9.,]', '', 'g'),
                        ',',
                        '.'
                    )::NUMERIC

            ELSE 1
        END AS quantity_value_raw
    FROM renamed
    WHERE LOWER(category) IN ('bebidas', 'carnes, aves e peixes', 'frios e congelados', 'hortifrúti', 'limpeza', 'mercearia', 'padaria e matinais')
)

SELECT * FROM units
