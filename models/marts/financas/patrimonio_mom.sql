{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}


WITH
ativos AS (
    SELECT
        mes_base,
        (total_patrimonio_bruto::NUMERIC / NULLIF(LAG(total_patrimonio_bruto) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                         AS total_patrimonio_bruto,
        (total_patrimonio_liquido::NUMERIC / NULLIF(LAG(total_patrimonio_liquido) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                     AS total_patrimonio_liquido,
        (patrimonio_liquido_lucas::NUMERIC / NULLIF(LAG(patrimonio_liquido_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                     AS patrimonio_liquido_lucas,
        (saldo_bradesco_lucas::NUMERIC / NULLIF(LAG(saldo_bradesco_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                             AS saldo_bradesco_lucas,
        (saldo_bradesco_investimentos_lucas::NUMERIC / NULLIF(LAG(saldo_bradesco_investimentos_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3) AS saldo_bradesco_investimentos_lucas,
        (saldo_nubank_investimentos_lucas::NUMERIC / NULLIF(LAG(saldo_nubank_investimentos_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)     AS saldo_nubank_investimentos_lucas,
        (saldo_nubank_cashback_lucas::NUMERIC / NULLIF(LAG(saldo_nubank_cashback_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)               AS saldo_nubank_cashback_lucas,
        (saldo_bitcoin_lucas::NUMERIC / NULLIF(LAG(saldo_bitcoin_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                               AS saldo_bitcoin_lucas,
        (saldo_daycoval_lucas::NUMERIC / NULLIF(LAG(saldo_daycoval_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                             AS saldo_daycoval_lucas,
        (saldo_avenue_lucas::NUMERIC / NULLIF(LAG(saldo_avenue_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                                 AS saldo_avenue_lucas,
        (saldo_wise_lucas::NUMERIC / NULLIF(LAG(saldo_wise_lucas) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                                     AS saldo_wise_lucas,
        (patrimonio_liquido_jessica::NUMERIC / NULLIF(LAG(patrimonio_liquido_jessica) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                 AS patrimonio_liquido_jessica,
        (saldo_banco_brasil_jessica::NUMERIC / NULLIF(LAG(saldo_banco_brasil_jessica) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                 AS saldo_banco_brasil_jessica,
        (saldo_sofisa_investimentos_jessica::NUMERIC / NULLIF(LAG(saldo_sofisa_investimentos_jessica) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3) AS saldo_sofisa_investimentos_jessica,
        (saldo_itau_investimentos_jessica::NUMERIC / NULLIF(LAG(saldo_itau_investimentos_jessica) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)     AS saldo_itau_investimentos_jessica,
        (saldo_nubank_investimentos_jessica::NUMERIC / NULLIF(LAG(saldo_nubank_investimentos_jessica) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3) AS saldo_nubank_investimentos_jessica,
        (saldo_avenue_jessica::NUMERIC / NULLIF(LAG(saldo_avenue_jessica) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                             AS saldo_avenue_jessica,
        (vlr_carro::NUMERIC / NULLIF(LAG(vlr_carro) OVER (ORDER BY mes_base), 0) - 1)::NUMERIC(18, 3)                                                   AS vlr_carro

    FROM {{ ref('int_patrimonio_mensal') }}
)

SELECT * FROM ativos ORDER BY mes_base
