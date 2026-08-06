{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

{#-
  Mart auxiliar de INPUT do loop de classificação de camada (SCD2).
  Lista os ativos do mês atual (distintos) — investimentos de int_carteira mais
  as disponibilidades em conta de int_carteira_extra — já com a camada vigente
  preenchida: os já classificados vêm com a camada as-of de
  stg_carteira_classificacao; os novos vêm como 'NAO CLASSIFICADO' (ou
  'RESERVA ESTRATEGICA', no caso das disponibilidades, que já têm camada
  natural). O Lucas copia estas linhas para a aba
  "classificacao" da planilha, que ACUMULA histórico: para classificar um ativo
  novo, preenche a camada da linha (com o mes_base atual); para reclassificar,
  ACRESCENTA uma nova linha com o mes_base atual e a nova camada, mantendo as
  linhas antigas (a chave da posição — pessoa, ativo, categoria, tipo,
  instituicao — nunca é editada). O ETL do Google lê a aba de volta para
  raw.carteira_classificacao; o downstream faz join as-of (última classificação
  com mes_base <= o mês da posição), fechando o loop e preservando a composição
  por camada ao longo do tempo.
-#}
{#- União por colunas nomeadas, não SELECT *: os dois modelos têm o mesmo layout,
    mas casar por posição já trocou camada por ativo em silêncio (ambas TEXT). Só
    as colunas usadas abaixo entram. -#}
WITH
base AS (
    SELECT
        mes_base,
        pessoa,
        ativo,
        indexador,
        classe_ativo,
        tipo_ativo,
        instituicao,
        data_vencimento,
        camada,
        fl_mes_atual
    FROM {{ ref('int_carteira') }}

    UNION ALL

    SELECT
        mes_base,
        pessoa,
        ativo,
        indexador,
        classe_ativo,
        tipo_ativo,
        instituicao,
        data_vencimento,
        camada,
        fl_mes_atual
    FROM {{ ref('int_carteira_extra') }}
),

final AS (
    SELECT DISTINCT
        mes_base,
        pessoa,
        ativo,
        indexador,
        classe_ativo,
        tipo_ativo,
        instituicao,
        data_vencimento,
        camada
    FROM base
    WHERE fl_mes_atual IS TRUE
)

SELECT
    final.*
FROM final
ORDER BY pessoa, camada, classe_ativo, tipo_ativo, ativo
