{{
  config(
    materialized = 'table',
    tags = ['financas', 'staging'],
  )
}}

WITH
source AS (
    SELECT
        TO_DATE(
            LPAD(data::TEXT, 8, '0'),
            'DDMMYYYY'
        )                                                   AS data_usd,
        venda::NUMERIC / POWER(10, LENGTH(venda::TEXT) - 1) AS vlr_usd
    FROM {{ ref('cotacao_usd') }}
),

ultimo_dia_disponivel AS (
    SELECT DISTINCT ON (DATE_TRUNC('month', data_usd))
        data_usd,
        vlr_usd::NUMERIC(18, 2) AS vlr_usd
    FROM source
    ORDER BY DATE_TRUNC('month', data_usd), data_usd DESC
),

final AS (
    SELECT
        (
            DATE_TRUNC('month', data_usd)
            + INTERVAL '1 month'
            - INTERVAL '1 day'
        )::DATE AS data_referencia,
        vlr_usd
    FROM ultimo_dia_disponivel
    ORDER BY data_referencia
)

SELECT * FROM final
