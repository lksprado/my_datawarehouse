{{
  config(
    materialized = 'table',
    tags = ['inflacao', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw', 'minha_inflacao') }}
),

units AS (
    SELECT * FROM {{ ref('stg_minha_inflacao') }}
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

final AS (
    SELECT
        (
            MAKE_DATE(t1.year, t1.month, 1)
            + INTERVAL '1 month'
            - INTERVAL '1 day'
        )::DATE                                                           AS created_at,
        {{ dbt_utils.generate_surrogate_key(['t1.clean_product_name']) }} AS sk_product,
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
    FROM renamed AS t1
    LEFT JOIN units AS t2
        ON {{ dbt_utils.generate_surrogate_key(['t1.clean_product_name']) }} = t2.sk_product
)

SELECT * FROM final
