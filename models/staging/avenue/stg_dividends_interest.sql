{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw', 'dividends_interest') }}
),

renamed AS (
    SELECT
        TO_DATE(period_start, 'yyyy-MM-dd')        AS period_start,
        TO_DATE(period_end, 'yyyy-MM-dd')          AS period_end,
        debit::NUMERIC(18, 2)                      AS debit,
        credit::NUMERIC(18, 2)                     AS credit,
        person                                     AS pessoa
    FROM source
),

final AS (
    SELECT
        CASE
            WHEN period_end - period_start > 31 THEN DATE_TRUNC('month', period_end)
            ELSE period_start
        END::DATE AS period_start,
        period_end,
        credit-debit AS vlr_liquido,
        pessoa
    FROM renamed
)

SELECT * FROM final
