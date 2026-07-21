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
        month_end_date,
        quarter_of_year,
        year_number
    FROM {{ ref('dim_datas') }}
),

variavel AS (
    SELECT
        instituicao,
        NULL::TEXT AS emissor,
        NULL::TEXT AS conglomerado,
        categoria_investimento,
        tipo_investimento,
        NULL::TEXT AS camada,
        investimento,
        NULL::TEXT AS indexador,
        NULL::DATE AS data_vencimento,
        NULL::INT AS vencimento_em_dias,
        vlr_atualizado,
        mes_base,
        pessoa
    FROM {{ ref('int_renda_variavel') }}
),

fixa AS (
    SELECT
        instituicao,
        emissor,
        conglomerado,
        categoria_investimento,
        tipo_investimento,
        NULL::TEXT AS camada,
        investimento,
        indexador,
        data_vencimento,
        vencimento_em_dias,
        vlr_atualizado,
        mes_base,
        pessoa
    FROM {{ ref('int_renda_fixa') }}
),

unioned AS (
    SELECT * FROM variavel
    UNION ALL
    SELECT * FROM fixa
),
mes_base_mais_recente AS (
    SELECT
    MAX(mes_base) as mes_base_mais_recente
    FROM unioned
),
final AS (
    SELECT
        t2.month_start_date AS mes,
        t2.month_end_date   AS mes_final,
        t2.quarter_of_year  AS semestre,
        t2.year_number      AS ano,
        t1.instituicao,
        t1.emissor,
        t1.conglomerado,
        t1.categoria_investimento,
        t1.tipo_investimento,
        t1.camada,
        t1.investimento,
        t1.indexador,
        t1.data_vencimento,
        t1.vencimento_em_dias,
        t1.vlr_atualizado,
        t1.mes_base,
        CASE 
            WHEN t3.mes_base_mais_recente IS NULL THEN false
            ELSE true
        END AS fl_mes_atual,
        t1.pessoa
    FROM unioned AS t1
    INNER JOIN datas AS t2
        ON t1.mes_base = t2.month_start_date
    LEFT JOIN mes_base_mais_recente t3
        ON t1.mes_base = t3.mes_base_mais_recente
    WHERE pessoa = 'deusa'
)

SELECT * FROM final
ORDER BY mes_base, pessoa, instituicao, categoria_investimento, tipo_investimento, investimento
