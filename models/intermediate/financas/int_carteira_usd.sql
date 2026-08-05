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
        CASE 
            WHEN symbol_cusip in ('TFLO', 'GOVT') and pessoa = 'lucas' then 'deusa'
            WHEN description ='US TREASURY' and pessoa = 'lucas' then 'deusa'
            ELSE pessoa 
        END AS pessoa,
        market_value
    FROM {{ ref('stg_assets') }}
),
assets_corrigido AS (
    SELECT
        period_start,
        period_end,
        pessoa,
        SUM(market_value)::INT AS vlr_liquido_usd
    FROM assets
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
    SELECT * FROM assets_corrigido
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
