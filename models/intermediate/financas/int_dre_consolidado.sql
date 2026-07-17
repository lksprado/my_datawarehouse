{{
  config(
    tags = ['investimentos', 'intermediate'],
  )
}}


WITH
lucas AS (
    SELECT * FROM {{ ref('stg_luc_consolidado') }}
),

jessica AS (
    SELECT * FROM {{ ref('stg_jsc_consolidado') }}
),

unioned AS (
    SELECT * FROM lucas
    UNION ALL
    SELECT * FROM jessica
)

SELECT * FROM unioned
ORDER BY
    mes_debito, pessoa
