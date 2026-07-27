{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

WITH
-- Um registro por mês. dim_datas tem uma linha por dia e `motivo` varia dentro
-- do mês, então DISTINCT com motivo na chave duplicaria o mês no join abaixo e
-- repetiria o valor cheio em cada linha.
datas AS (
    SELECT
        month_start_date,
        MAX(quarter_of_year) AS quarter_of_year,
        MAX(year_number)     AS year_number,
        MAX(fl_mes_especial) AS fl_mes_especial,
        COALESCE(
            STRING_AGG(DISTINCT NULLIF(motivo, 'NORMAL'), ' / '),
            'NORMAL'
        )                    AS motivo
    FROM {{ ref('dim_datas') }}
    GROUP BY month_start_date
),

consolidados AS (
    SELECT
        t1.mes_debito,
        t2.fl_mes_especial,
        t2.motivo,
        t2.quarter_of_year                          AS trimestre,
        t2.year_number                              AS ano,
        SUM(t1.receita_total)                       AS total_receita,
        SUM(t1.despesas_total)                      AS total_despesas,
        SUM(t1.receita_total) - SUM(despesas_total) AS resultado,
        SUM(t1.ajuste_realizado)                    AS ajuste_realizado,
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
        t2.fl_mes_especial,
        t2.motivo,
        t2.quarter_of_year,
        t2.year_number
    ORDER BY t1.mes_debito
)

SELECT
    consolidados.*,
    CURRENT_TIMESTAMP AS model_updated_at
FROM consolidados
