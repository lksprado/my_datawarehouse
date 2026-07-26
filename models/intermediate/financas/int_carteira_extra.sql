{{
  config(
    materialized = 'table',
    tags = ['financas', 'intermediate'],
  )
}}

{#-
  Disponibilidades em conta (saldo Bradesco/Wise/Bitcoin do Lucas, Banco do
  Brasil da Jéssica) derivadas de int_patrimonio_mensal, no mesmo layout de
  int_carteira para que os dois possam ser unidos por posição a jusante.

  A camada vem do mesmo join as-of (SCD2) contra stg_carteira_classificacao que
  int_carteira usa, fechando o loop da planilha também para as disponibilidades.
  O fallback aqui é 'RESERVA ESTRATEGICA' (e não 'NAO CLASSIFICADO' como em
  int_carteira) porque saldo em conta já tem camada natural — não deve aparecer
  como pendência de classificação na planilha.
-#}

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
  'DISPONIBILIDADE'    AS classe_ativo,
  'CONTA CORRENTE' AS tipo_ativo,
  'SALDO EM CONTA' AS ativo,
  'TIR' AS indexador,
  NULL::DATE AS data_vencimento,
  NULL::int AS vencimento_em_dias,
  saldo_bradesco_lucas as vlr_atualizado_brl,
  'BRL' as moeda_ativo,
  fl_mes_atual,
  'lucas' as pessoa
  FROM {{ ref('int_patrimonio_mensal') }}
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
  'DISPONIBILIDADE'    AS classe_ativo,
  'CONTA CORRENTE' AS tipo_ativo,
  'SALDO EM CONTA' AS ativo,
  NULL::TEXT AS indexador,
  NULL::DATE AS data_vencimento,
  NULL::int AS vencimento_em_dias,
  saldo_wise_lucas as vlr_atualizado_brl,
  'USD' as moeda_ativo,
  fl_mes_atual,
  'lucas' as pessoa
  FROM {{ ref('int_patrimonio_mensal') }}
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
  'DISPONIBILIDADE'    AS classe_ativo,
  'CONTA CORRENTE' AS tipo_ativo,
  'SALDO EM CONTA' AS ativo,
  NULL::TEXT AS indexador,
  NULL::DATE AS data_vencimento,
  NULL::int AS vencimento_em_dias,
  saldo_bitcoin_lucas as vlr_atualizado_brl,
  'BTC' as moeda_ativo,
  fl_mes_atual,
  'lucas' as pessoa
  FROM {{ ref('int_patrimonio_mensal') }}
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
  'DISPONIBILIDADE'    AS classe_ativo,
  'CONTA CORRENTE' AS tipo_ativo,
  'SALDO EM CONTA' AS ativo,
  'TIR' AS indexador,
  NULL::DATE AS data_vencimento,
  NULL::int AS vencimento_em_dias,
  saldo_banco_brasil_jessica as vlr_atualizado_brl,
  'BRL' as moeda_ativo,
  fl_mes_atual,
  'jessica' as pessoa
  FROM {{ ref('int_patrimonio_mensal') }}
),

unioned AS (
  select * from ativos_bradesco_lucas
  union all
  select * from ativos_wise_lucas
  union all
  select * from ativos_bitcoin_lucas
  union all
  select * from ativos_bb_jessica
),

{#- A ordem das colunas abaixo é idêntica à de int_carteira: carteira_lucas_jessica
    une os dois com SELECT *, que casa por posição. -#}
final AS (
    SELECT
        t1.mes_base,
        t1.mes_final,
        t1.trimestre,
        t1.ano,
        t1.instituicao,
        t1.emissor,
        t1.conglomerado,
        t1.classe_ativo,
        t1.tipo_ativo,
        COALESCE(cls.camada, 'NAO CLASSIFICADO')     AS camada,
        t1.ativo,
        COALESCE(t1.indexador, cls.indexador)        AS indexador,
        t1.data_vencimento,
        t1.vencimento_em_dias,
        t1.vlr_atualizado_brl,
        t1.moeda_ativo,
        t1.fl_mes_atual,
        t1.pessoa
    FROM unioned AS t1
    LEFT JOIN LATERAL (
        SELECT c.camada, c.indexador
        FROM {{ ref('stg_carteira_classificacao') }} AS c
        WHERE c.pessoa = t1.pessoa
          AND c.ativo = t1.ativo
          AND c.classe_ativo = t1.classe_ativo
          AND c.tipo_ativo = t1.tipo_ativo
          AND c.instituicao = t1.instituicao
          AND c.mes_base <= t1.mes_base
        ORDER BY c.mes_base DESC
        LIMIT 1
    ) AS cls ON TRUE
)

SELECT * FROM final
ORDER BY mes_base, pessoa, instituicao, classe_ativo, tipo_ativo, ativo
