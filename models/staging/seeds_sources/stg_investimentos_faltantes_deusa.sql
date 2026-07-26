{{
  config(
    materialized = 'table',
    tags = ['financas', 'staging'],
  )
}}

WITH
seed AS (
    SELECT * FROM {{ ref('investimentos_faltantes_deusa') }}
),

renamed AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        NULL::TEXT AS emissor,
        classe_ativo,
        tipo_ativo,
        ativo,
        indexador,
        NULL::DATE     AS data_emissao,
        data_vencimento,
        vlr_atualizado AS vlr_atualizado_brl,
        'BRL'          AS moeda_ativo
    FROM seed
    ORDER BY mes_base, pessoa, instituicao, ativo
)

SELECT * FROM renamed
