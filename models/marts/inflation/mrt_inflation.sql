{{
  config(
    tags = ['inflacao','marts'],
    )
}}

WITH
dim AS (
    SELECT * FROM {{ ref('int_dim_products') }}
),

fct AS (
    SELECT * FROM {{ ref('int_fct_products') }}
),

final AS (
    SELECT
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
    FROM
        fct
    INNER JOIN dim
        ON fct.sku = dim.sku
)

SELECT * FROM final
ORDER BY
    created_at ASC,
    sku DESC
