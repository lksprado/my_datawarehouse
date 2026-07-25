{{
  config(
    tags = ['financas', 'intermediate'],
  )
}}

WITH
ativos AS (
    SELECT
        mes,
        total_patrimonio_bruto,
        total_patrimonio_liquido,

        patrimonio_liquido_lucas,
        saldo_bradesco_lucas,
        saldo_bradesco_investimentos_lucas,
        saldo_nubank_investimentos_lucas,
        saldo_nubank_cashback_lucas,
        saldo_bitcoin_lucas,
        saldo_daycoval_lucas,
        saldo_avenue_lucas,
        saldo_wise_lucas,

        patrimonio_liquido_jessica,
        saldo_banco_brasil_jessica,
        saldo_sofisa_investimentos_jessica,
        saldo_itau_investimentos_jessica,
        saldo_nubank_investimentos_jessica,
        saldo_avenue_jessica
    FROM {{ ref('stg_ativos') }}
    ORDER BY mes
)

SELECT * FROM ativos
