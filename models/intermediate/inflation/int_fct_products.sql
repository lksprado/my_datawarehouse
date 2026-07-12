{{
  config(
    materialized = 'view',
    tags = ['inflacao', 'intermediate'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ ref('stg_atacadao') }}
),

final AS (
    SELECT DISTINCT
        created_at,
        sku,
        high_price,
        low_price
    FROM source
)

SELECT * FROM final
