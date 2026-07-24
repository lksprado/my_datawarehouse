{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

{#-
  Mart auxiliar de INPUT do loop de classificação de camada (SCD2).
  Lista os ativos do mês atual (distintos) já com a camada vigente preenchida:
  os já classificados vêm com a camada as-of de stg_carteira_classificacao; os
  novos vêm como 'NAO CLASSIFICADO'. O Lucas copia estas linhas para a aba
  "classificacao" da planilha, que ACUMULA histórico: para classificar um ativo
  novo, preenche a camada da linha (com o mes_base atual); para reclassificar,
  ACRESCENTA uma nova linha com o mes_base atual e a nova camada, mantendo as
  linhas antigas (a chave da posição — pessoa, investimento, categoria, tipo,
  instituicao — nunca é editada). O ETL do Google lê a aba de volta para
  raw.carteira_classificacao; o downstream faz join as-of (última classificação
  com mes_base <= o mês da posição), fechando o loop e preservando a composição
  por camada ao longo do tempo.
-#}
WITH
final AS (
    SELECT DISTINCT
        mes_base,
        pessoa,
        investimento,
        categoria_investimento,
        tipo_investimento,
        instituicao,
        camada
    FROM {{ ref('int_carteira') }}
    WHERE fl_mes_atual IS TRUE
)

SELECT * FROM final
ORDER BY pessoa, camada, categoria_investimento, tipo_investimento, investimento
