{{
  config(
    materialized = 'ephemeral',
    tags = ['inflacao', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw', 'minha_inflacao') }}
),

renamed AS (
    SELECT
        "Produto"                                                               AS product_name,
        "Unidade"                                                               AS product_unity,
        "Qtd"                                                                   AS quantity,
        "Preço"                                                                 AS price,
        "Mês Atual"                                                             AS current_month,
        "Peso"                                                                  AS weight,
        "Mês passado"                                                           AS past_month,
        "Var"                                                                   AS price_variation,
        "Var Peso"                                                              AS weight_variation,
        "Categoria"                                                             AS category,
        "Mes"                                                                   AS month,
        "Ano"                                                                   AS year,
        REGEXP_REPLACE(public.unaccent(LOWER("Produto")), '[^a-z0-9]', '', 'g') AS clean_product_name
    FROM source
),

units AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['clean_product_name']) }} AS sk_product,
        REPLACE(LOWER(public.unaccent(product_name)), ' ', '_')          AS product_name,
        REPLACE(TRIM(LOWER(product_unity)), '.', ',')                    AS product_unity,
        CASE
        -- PESO
            WHEN LOWER(product_unity) ~* '(kg|quilo|grama|g\\b)' THEN 'weight'
            -- VOLUME
            WHEN LOWER(product_unity) ~* '(litro|l\\b|ml)' THEN 'volume'
            -- UNIDADE / EMBALAGEM CONTÁVEL
            WHEN LOWER(product_unity) ~* '(fardo|caixa|pacote|barra|pote|maço|unidade|sabonete)' THEN 'unit'
            ELSE 'unknown'
        END                                                            AS quantity_type,
        -- VALOR NUMÉRICO BRUTO
        CASE
            WHEN LOWER(product_unity) ~* '^(kg|quilo|litro|l|ml)$' THEN 1::NUMERIC

            WHEN LOWER(product_unity) ~* '[0-9]'
                THEN
                    REPLACE(
                        REGEXP_REPLACE(LOWER(product_unity), '[^0-9.,]', '', 'g'),
                        ',',
                        '.'
                    )::NUMERIC

            ELSE 1
        END                                                            AS quantity_value_raw
    FROM renamed
),

final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_name) AS rn,

        -- VALOR PADRONIZADO (g / ml / unidades)
        CASE
            WHEN LOWER(product_unity) ~* 'kg' THEN quantity_value_raw * 1000
            WHEN LOWER(product_unity) ~* '(grama|g\b)' THEN quantity_value_raw
            WHEN LOWER(product_unity) ~* 'litro' THEN quantity_value_raw * 1000
            WHEN LOWER(product_unity) ~* 'ml' THEN quantity_value_raw
            WHEN quantity_type = 'unit' THEN quantity_value_raw
            ELSE 1
        END                                           AS quantity_value_std
    FROM units
)

SELECT
    sk_product,
    product_name,
    product_unity,
    quantity_type,
    quantity_value_raw,
    quantity_value_std
FROM final
WHERE rn = 2
