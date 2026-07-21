{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}


WITH
datas AS (
    SELECT DISTINCT
        month_start_date,
        quarter_of_year,
        year_number
    FROM {{ ref('dim_datas') }}
),

ativos AS (
    SELECT
        t1.mes,
        t2.quarter_of_year AS semestre,
        t2.year_number     AS ano,
        t1.total_patrimonio_bruto,
        t1.total_patrimonio_liquido,
        t1.patrimonio_liquido_lucas,
        t1.saldo_bradesco,
        t1.saldo_bradesco_investimentos,
        t1.saldo_nubank_investimentos_lucas,
        t1.saldo_nubank_cashback,
        t1.saldo_bitcoin,
        t1.saldo_daycoval,
        t1.saldo_avenue_lucas,
        t1.saldo_wise,
        t1.patrimonio_liquido_jessica,
        t1.saldo_banco_brasil,
        t1.saldo_sofisa_investimentos,
        t1.saldo_itau_investimentos,
        t1.saldo_nubank_investimentos_jessica,
        t1.saldo_avenue_jessica,
        t1.inflacao,
        t1.selic,
        t1.curva_inflacao,
        t1.curva_juros
    FROM {{ ref('int_ativos_agregado') }} AS t1
    INNER JOIN datas AS t2
        ON t1.mes = t2.month_start_date
)

SELECT * FROM ativos
