{{
  config(
    materialized = 'table',
    tags = ['financas', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw', 'assets') }}
),

renamed AS (
    SELECT
        TO_DATE(period_start, 'yyyy-MM-dd')        AS period_start,
        TO_DATE(period_end, 'yyyy-MM-dd')          AS period_end,
        {{ clean_string("asset_class", "upper") }} AS asset_class,
        {{ clean_string("description", "upper") }} AS description,
        symbol_cusip,
        market_value::NUMERIC(18, 2)                AS market_value,
        person                                      AS pessoa
    FROM source
),

final AS (
    SELECT
        CASE
            WHEN period_end - period_start > 31 THEN DATE_TRUNC('month', period_end)
            ELSE period_start
        END::DATE AS period_start,
        period_end,
        asset_class,
        market_value,
        CASE 
            WHEN symbol_cusip IN ('TFLO', 'GOVT') AND pessoa = 'lucas' THEN 'deusa'
            WHEN symbol_cusip IN ('91282CLH2', '7009170', '7381496', '7009637') AND pessoa = 'lucas' THEN 'deusa'
            ELSE pessoa 
        END AS pessoa,
        CASE
            WHEN symbol_cusip IN ('91282CLH2', '7009170', '7381496', '7009637')
                THEN 'US TREASURY'
            ELSE description
        END       AS description,
        CASE
            WHEN symbol_cusip = '91282CLH2'
                THEN '7009170'
            ELSE symbol_cusip
        END       AS symbol_cusip,
        'USD'     AS moeda_ativo
    FROM renamed
)

SELECT * FROM final
