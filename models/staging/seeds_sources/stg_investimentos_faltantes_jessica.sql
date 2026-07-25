{{
  config(
    materialized = 'table',
    tags = ['financas', 'staging'],
  )
}}

WITH
seed AS (
    SELECT * FROM {{ ref('investimentos_faltantes_jessica') }}
),

renamed AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        NULL::TEXT AS emissor,
        categoria_investimento,
        tipo_investimento,
        investimento,
        indexador,
        NULL::DATE     AS data_emissao,
        data_vencimento,
        vlr_atualizado AS vlr_atualizado_brl,
        'BRL'          AS moeda_ativo
    FROM seed
    ORDER BY mes_base, instituicao, investimento
)

SELECT * FROM renamed
