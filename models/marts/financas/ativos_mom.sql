{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}


WITH
ativos AS (
    SELECT
        mes,
        (total_patrimonio_bruto::NUMERIC / NULLIF(LAG(total_patrimonio_bruto) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                         AS total_patrimonio_bruto,
        (total_patrimonio_liquido::NUMERIC / NULLIF(LAG(total_patrimonio_liquido) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                     AS total_patrimonio_liquido,
        (patrimonio_liquido_lucas::NUMERIC / NULLIF(LAG(patrimonio_liquido_lucas) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                     AS patrimonio_liquido_lucas,
        (saldo_bradesco::NUMERIC / NULLIF(LAG(saldo_bradesco) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                                         AS saldo_bradesco,
        (saldo_bradesco_investimentos::NUMERIC / NULLIF(LAG(saldo_bradesco_investimentos) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)             AS saldo_bradesco_investimentos,
        (saldo_nubank_investimentos_lucas::NUMERIC / NULLIF(LAG(saldo_nubank_investimentos_lucas) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)     AS saldo_nubank_investimentos_lucas,
        (saldo_nubank_cashback::NUMERIC / NULLIF(LAG(saldo_nubank_cashback) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                           AS saldo_nubank_cashback,
        (saldo_bitcoin::NUMERIC / NULLIF(LAG(saldo_bitcoin) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                                           AS saldo_bitcoin,
        (saldo_daycoval::NUMERIC / NULLIF(LAG(saldo_daycoval) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                                         AS saldo_daycoval,
        (saldo_avenue_lucas::NUMERIC / NULLIF(LAG(saldo_avenue_lucas) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                                 AS saldo_avenue_lucas,
        (saldo_wise::NUMERIC / NULLIF(LAG(saldo_wise) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                                                 AS saldo_wise,
        (patrimonio_liquido_jessica::NUMERIC / NULLIF(LAG(patrimonio_liquido_jessica) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                 AS patrimonio_liquido_jessica,
        (saldo_banco_brasil::NUMERIC / NULLIF(LAG(saldo_banco_brasil) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                                 AS saldo_banco_brasil,
        (saldo_sofisa_investimentos::NUMERIC / NULLIF(LAG(saldo_sofisa_investimentos) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                 AS saldo_sofisa_investimentos,
        (saldo_itau_investimentos::NUMERIC / NULLIF(LAG(saldo_itau_investimentos) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                     AS saldo_itau_investimentos,
        (saldo_nubank_investimentos_jessica::NUMERIC / NULLIF(LAG(saldo_nubank_investimentos_jessica) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3) AS saldo_nubank_investimentos_jessica,
        (saldo_avenue_jessica::NUMERIC / NULLIF(LAG(saldo_avenue_jessica) OVER (ORDER BY mes), 0) - 1)::NUMERIC(18, 3)                             AS saldo_avenue_jessica

    FROM {{ ref('int_ativos_agregado') }}
)

SELECT * FROM ativos
