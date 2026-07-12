{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

WITH
seed AS (
    SELECT * FROM {{ ref('investimentos_faltantes') }}
),

renamed AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        categoria_investimento,
        tipo_investimento,
        investimento,
        indexador,
        NULL::DATE AS data_emissao,
        data_vencimento,
        vlr_atualizado
    FROM seed
    ORDER BY mes_base, pessoa, instituicao, investimento
)

SELECT * FROM renamed
