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
        saldo_bradesco,
        saldo_bradesco_investimentos,
        saldo_nubank_investimentos_lucas,
        saldo_nubank_cashback,
        saldo_bitcoin,
        saldo_daycoval,
        saldo_avenue_lucas,
        saldo_wise,

        patrimonio_liquido_jessica,
        saldo_banco_brasil,
        saldo_sofisa_investimentos,
        saldo_itau_investimentos,
        saldo_nubank_investimentos_jessica,
        saldo_avenue_jessica
    FROM {{ ref('stg_ativos') }}
    ORDER BY mes
)

SELECT * FROM ativos
