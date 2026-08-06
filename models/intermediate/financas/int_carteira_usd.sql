{{
  config(
    materialized = 'table',
    tags = ['financas', 'intermediate'],
  )
}}

WITH
assets AS (
    SELECT
        period_start,
        period_end,
        pessoa,
        SUM(market_value)::INT AS vlr_liquido_usd
    FROM {{ ref('stg_assets') }}
    GROUP BY
        period_start,
        period_end,
        pessoa
),

dividends AS (
    SELECT
        period_start,
        period_end,
        pessoa,
        SUM(vlr_liquido_usd)::INT AS vlr_liquido_usd
    FROM {{ ref('stg_dividends_interest') }}
    GROUP BY
        period_start,
        period_end,
        pessoa
),

unioned AS (
    SELECT * FROM assets
    UNION ALL
    SELECT * FROM dividends
),

final AS (
    SELECT
        period_start,
        period_end,
        pessoa,
        SUM(vlr_liquido_usd) AS vlr_liquido_usd
    FROM unioned
    GROUP BY
        period_start,
        period_end,
        pessoa
    ORDER BY period_start
)

SELECT * FROM final
