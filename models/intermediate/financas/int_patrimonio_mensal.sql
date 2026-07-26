{{
  config(
    tags = ['financas', 'intermediate'],
  )
}}

WITH
datas AS (
    SELECT DISTINCT
        month_start_date,
        month_end_date,
        quarter_of_year,
        year_number
    FROM {{ ref('dim_datas') }}
),

mes_base_mais_recente AS (
    SELECT
    MAX(mes_base) as mes_base_mais_recente
    FROM {{ ref('stg_patrimonio') }}
),

ativos AS (
    SELECT
        t1.mes_base,
        t2.month_end_date   AS mes_final,
        t2.quarter_of_year  AS trimestre,
        t2.year_number      AS ano,
        CASE
            WHEN t3.mes_base_mais_recente IS NULL THEN false
            ELSE true
        END AS fl_mes_atual,
        t1.total_patrimonio_bruto,
        t1.total_patrimonio_liquido,

        t1.patrimonio_liquido_lucas,
        t1.saldo_bradesco_lucas,
        t1.saldo_bradesco_investimentos_lucas,
        t1.saldo_nubank_investimentos_lucas,
        t1.saldo_nubank_cashback_lucas,
        t1.saldo_bitcoin_lucas,
        t1.saldo_daycoval_lucas,
        t1.saldo_avenue_lucas,
        t1.saldo_wise_lucas,

        t1.patrimonio_liquido_jessica,
        t1.saldo_banco_brasil_jessica,
        t1.saldo_sofisa_investimentos_jessica,
        t1.saldo_itau_investimentos_jessica,
        t1.saldo_nubank_investimentos_jessica,
        t1.saldo_avenue_jessica,
        t1.vlr_carro
    FROM {{ ref('stg_patrimonio') }} t1
    INNER JOIN datas t2 
    ON t1.mes_base = t2.month_start_date
    LEFT JOIN mes_base_mais_recente t3
        ON t1.mes_base = t3.mes_base_mais_recente
    ORDER BY mes_base
)

SELECT * FROM ativos
