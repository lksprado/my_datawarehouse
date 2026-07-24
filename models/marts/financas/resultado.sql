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

consolidados AS (
    SELECT
        t1.mes_debito,
        t2.quarter_of_year                          AS trimestre,
        t2.year_number                              AS ano,
        SUM(t1.receita_total)                       AS total_receita,
        SUM(t1.despesas_total)                      AS total_despesas,
        SUM(t1.receita_total) - SUM(despesas_total) AS resultado,
        SUM(t1.salario)                             AS total_salario,
        SUM(t1.dividendos)                          AS total_dividendo,
        SUM(t1.outros)                              AS total_outros,
        SUM(t1.mercado)                             AS total_mercado,
        SUM(t1.diversos)                            AS total_diversos,
        SUM(t1.assinaturas)                         AS total_assinaturas,
        SUM(t1.role)                                AS total_role,
        SUM(t1.transporte)                          AS total_transporte,
        SUM(t1.apartamento)                         AS total_apartamento,
        SUM(t1.saude)                               AS total_saude,
        SUM(t1.educacao)                            AS total_educacao

    FROM {{ ref('int_dre_consolidado') }} AS t1
    INNER JOIN datas AS t2
        ON t1.mes_debito = t2.month_start_date
    WHERE t1.mes_debito > '2023-08-01'
    GROUP BY
        t1.mes_debito,
        t2.quarter_of_year,
        t2.year_number
    ORDER BY t1.mes_debito
)

SELECT * FROM consolidados
