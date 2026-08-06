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
    FROM {{ ref('stg_patrimonio_deusa') }}
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
        t1.total_patrimonio_liquido,

        t1.saldo_banco_brasil_deusa,
        t1.saldo_banco_brasil_investimentos_deusa,
        t1.saldo_bradesco_deusa,
        t1.saldo_bradesco_investimentos_deusa,
        t1.saldo_nubank_deusa,
        t1.saldo_nubank_investimentos_deusa,
        t1.saldo_nubank_cashback_deusa        
    FROM {{ ref('stg_patrimonio_deusa') }} t1
    INNER JOIN datas t2 
    ON t1.mes_base = t2.month_start_date
    LEFT JOIN mes_base_mais_recente t3
        ON t1.mes_base = t3.mes_base_mais_recente
    ORDER BY mes_base
)

SELECT * FROM ativos
