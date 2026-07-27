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
        mes_final,
        trimestre,
        ano,
        fl_mes_atual,
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
        saldo_avenue_jessica,
        vlr_carro
    FROM {{ ref('int_patrimonio_mensal') }}
)

SELECT
    ativos.*,
    CURRENT_TIMESTAMP AS model_updated_at
FROM ativos ORDER BY mes_base
