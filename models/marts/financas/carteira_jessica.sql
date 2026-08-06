{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

{#- União por colunas nomeadas, não SELECT *: os dois modelos têm o mesmo layout,
    mas casar por posição já trocou camada por ativo em silêncio (ambas TEXT). -#}
WITH
unioned AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        instituicao,
        emissor,
        conglomerado,
        classe_ativo,
        tipo_ativo,
        camada,
        ativo,
        indexador,
        data_vencimento,
        vencimento_em_dias,
        vlr_atualizado_brl,
        moeda_ativo,
        fl_mes_atual,
        pessoa
    FROM {{ ref('int_carteira') }}

    UNION ALL

    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        instituicao,
        emissor,
        conglomerado,
        classe_ativo,
        tipo_ativo,
        camada,
        ativo,
        indexador,
        data_vencimento,
        vencimento_em_dias,
        vlr_atualizado_brl,
        moeda_ativo,
        fl_mes_atual,
        pessoa
    FROM {{ ref('int_carteira_extra') }}
)
select
    unioned.*,
    CURRENT_TIMESTAMP AS model_updated_at
from unioned WHERE pessoa = 'jessica'
ORDER BY mes_base, pessoa, instituicao, classe_ativo, tipo_ativo, ativo
