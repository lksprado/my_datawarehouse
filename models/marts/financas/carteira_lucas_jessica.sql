{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

WITH
ativos_bradesco_lucas AS (
  SELECT 
  mes_base,
  mes_final,
  trimestre,
  ano,
  'BRADESCO' AS instituicao,
  NULL::TEXT AS emissor,
  'BRADESCO' AS conglomerado,
  'DISPONIBILIDADE'    AS categoria_investimento,
  'CONTA CORRENTE' AS tipo_investimento,
  'RESERVA ESTRATEGICA' AS camada,
  'SALDO EM CONTA' AS investimento,
  'TIR' AS indexador,
  NULL::DATE AS data_vencimento,
  NULL::int AS vencimento_em_dias,
  saldo_bradesco_lucas as vlr_atualizado_brl,
  'BRL' as moeda_ativo,
  fl_mes_atual,
  'lucas' as pessoa
  FROM {{ ref('int_ativos_agregado') }}
),

ativos_wise_lucas AS (
  SELECT 
  mes_base,
  mes_final,
  trimestre,
  ano,
  'WISE' AS instituicao,
  NULL::TEXT AS emissor,
  NULL::TEXT AS conglomerado,
  'DISPONIBILIDADE'    AS categoria_investimento,
  'CONTA CORRENTE' AS tipo_investimento,
  'RESERVA ESTRATEGICA' AS camada,
  'SALDO EM CONTA' AS investimento,
  NULL::TEXT AS indexador,
  NULL::DATE AS data_vencimento,
  NULL::int AS vencimento_em_dias,
  saldo_wise_lucas as vlr_atualizado_brl,
  'USD' as moeda_ativo,
  fl_mes_atual,
  'lucas' as pessoa
  FROM {{ ref('int_ativos_agregado') }}
),

ativos_bitcoin_lucas AS (
  SELECT 
  mes_base,
  mes_final,
  trimestre,
  ano,
  'AUTOCUSTODIA' AS instituicao,
  NULL::TEXT AS emissor,
  NULL::TEXT AS conglomerado,
  'DISPONIBILIDADE'    AS categoria_investimento,
  'CONTA CORRENTE' AS tipo_investimento,
  'RESERVA ESTRATEGICA' AS camada,
  'SALDO EM CONTA' AS investimento,
  NULL::TEXT AS indexador,
  NULL::DATE AS data_vencimento,
  NULL::int AS vencimento_em_dias,
  saldo_bitcoin_lucas as vlr_atualizado_brl,
  'BTC' as moeda_ativo,
  fl_mes_atual,
  'lucas' as pessoa
  FROM {{ ref('int_ativos_agregado') }}
),

ativos_bb_jessica AS (
  SELECT 
  mes_base,
  mes_final,
  trimestre,
  ano,
  'BANCO DO BRASIL' AS instituicao,
  NULL::TEXT AS emissor,
  'BANCO DO BRASIL' AS conglomerado,
  'DISPONIBILIDADE'    AS categoria_investimento,
  'CONTA CORRENTE' AS tipo_investimento,
  'RESERVA ESTRATEGICA' AS camada,
  'SALDO EM CONTA' AS investimento,
  'TIR' AS indexador,
  NULL::DATE AS data_vencimento,
  NULL::int AS vencimento_em_dias,
  saldo_banco_brasil_jessica as vlr_atualizado_brl,
  'BRL' as moeda_ativo,
  fl_mes_atual,
  'jessica' as pessoa
  FROM {{ ref('int_ativos_agregado') }}
),
unioned AS (
  SELECT * FROM {{ ref('int_carteira') }} 
  union all 
  select * from ativos_bradesco_lucas
  union all 
  select * from ativos_wise_lucas
  union all 
  select * from ativos_bitcoin_lucas
  union all 
  select * from ativos_bb_jessica
)
select * from unioned WHERE pessoa IN ('lucas', 'jessica')
ORDER BY mes_base, pessoa, instituicao, categoria_investimento, tipo_investimento, investimento
