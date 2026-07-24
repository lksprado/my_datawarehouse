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

luz AS (
    SELECT
        t1.mes,
        t2.quarter_of_year AS trimestre,
        t2.year_number     AS ano,
        t1.vlr_fatura,
        t1.kwh,
        t1.dias,
        t1.kwh_dia,
        t1.preco_kwh
    FROM {{ ref('stg_luz') }} AS t1
    INNER JOIN datas AS t2
        ON t1.mes = t2.month_start_date
)

SELECT * FROM luz
