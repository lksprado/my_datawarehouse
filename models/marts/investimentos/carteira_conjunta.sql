{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

WITH
variavel AS (
    SELECT
        (DATE_TRUNC('month', mes_base) + INTERVAL '1 month - 1 day')::DATE AS mes,
        instituicao,
        categoria_investimento,
        tipo_investimento,
        NULL::TEXT                                                         AS camada,
        investimento,
        NULL::TEXT                                                         AS indexador,
        NULL::DATE                                                         AS data_vencimento,
        vlr_atualizado,
        mes_base,
        pessoa
    FROM {{ ref('int_renda_variavel') }}
),

fixa AS (
    SELECT
        (DATE_TRUNC('month', mes_base) + INTERVAL '1 month - 1 day')::DATE AS mes,
        instituicao,
        categoria_investimento,
        tipo_investimento,
        NULL::TEXT                                                         AS camada,
        investimento,
        indexador,
        data_vencimento,
        vlr_atualizado,
        mes_base,
        pessoa
    FROM {{ ref('int_renda_fixa') }}
),

unioned AS (
    SELECT * FROM variavel
    UNION ALL
    SELECT * FROM fixa
)

SELECT * FROM unioned
ORDER BY mes_base, pessoa, instituicao, categoria_investimento, tipo_investimento, investimento
