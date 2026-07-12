{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

with 
seed as (
    select * from {{ref('investimentos_faltantes') }}
),
renamed as (
    select
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
    FROM SEED
    order by mes_base, pessoa, instituicao, investimento
)
SELECT * FROM renamed