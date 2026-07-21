{{
  config(
    tags = ['financas', 'intermediate'],
  )
}}


WITH
lucas AS (
    SELECT * FROM {{ ref('stg_luc_contas') }}
),

jessica AS (
    SELECT * FROM {{ ref('stg_jsc_contas') }}
),

unioned AS (
    SELECT * FROM lucas
    UNION ALL
    SELECT * FROM jessica
)

SELECT * FROM unioned
ORDER BY
    mes_debito, pessoa
