{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

WITH
consolidados AS (
    SELECT
        t1.mes_debito       AS mes,
        t1.data_debito      AS data,
        t1.nome_dia         AS dia_nome,
        t1.dia_ajustado     AS dia_fatura,
        t1.dia_real         AS dia_ano,
        t2.day_of_month     AS dia_mes,
        t2.week_of_year     AS semana,
        t2.quarter_of_year  AS trimestre,
        SUM(t1.mercado)     AS total_mercado,
        SUM(t1.diversos)    AS total_diversos,
        SUM(t1.assinaturas) AS total_assinaturas,
        SUM(t1.role)        AS total_role,
        SUM(t1.transporte)  AS total_transporte,
        SUM(t1.apartamento) AS total_apartamento,
        SUM(t1.saude)       AS total_saude,
        SUM(t1.educacao)    AS total_educacao
    FROM {{ ref('int_consumo_consolidado') }} AS t1
    INNER JOIN {{ ref('dim_datas') }} AS t2
        ON t1.data_debito = t2.date_day
    WHERE mes_debito > '2023-08-01'
    GROUP BY
        t1.mes_debito,
        t1.data_debito,
        t1.nome_dia,
        t1.dia_ajustado,
        t1.dia_real,
        t2.day_of_month,
        t2.week_of_year,
        t2.quarter_of_year
    ORDER BY data_debito
)

SELECT * FROM consolidados
