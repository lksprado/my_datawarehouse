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
  int_carteira usa, fechando o loop da planilha também para as disponibilidades,
  com o mesmo fallback 'NAO CLASSIFICADO'.
-#}

WITH
ativos_bradesco_lucas AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'BRADESCO'           AS instituicao,
        NULL::TEXT           AS emissor,
        'BRADESCO'           AS conglomerado,
        'DISPONIBILIDADE'    AS classe_ativo,
        'CONTA CORRENTE'     AS tipo_ativo,
        'SALDO EM CONTA'     AS ativo,
        'TIR'                AS indexador,
        NULL::DATE           AS data_vencimento,
        NULL::INT            AS vencimento_em_dias,
        saldo_bradesco_lucas AS vlr_atualizado_brl,
        'BRL'                AS moeda_ativo,
        fl_mes_atual,
        'lucas'              AS pessoa
    FROM {{ ref('int_patrimonio_mensal') }}
),

ativos_bradesco_deusa AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'BRADESCO'           AS instituicao,
        NULL::TEXT           AS emissor,
        'BRADESCO'           AS conglomerado,
        'DISPONIBILIDADE'    AS classe_ativo,
        'CONTA CORRENTE'     AS tipo_ativo,
        'SALDO EM CONTA'     AS ativo,
        'TIR'                AS indexador,
        NULL::DATE           AS data_vencimento,
        NULL::INT            AS vencimento_em_dias,
        saldo_bradesco_deusa AS vlr_atualizado_brl,
        'BRL'                AS moeda_ativo,
        fl_mes_atual,
        'deusa'              AS pessoa
    FROM {{ ref('int_patrimonio_mensal_deusa') }}
),

ativos_nubank_deusa AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'NUBANK'           AS instituicao,
        NULL::TEXT         AS emissor,
        'NUBANK'           AS conglomerado,
        'DISPONIBILIDADE'  AS classe_ativo,
        'CONTA CORRENTE'   AS tipo_ativo,
        'SALDO EM CONTA'   AS ativo,
        NULL::TEXT         AS indexador,
        NULL::DATE         AS data_vencimento,
        NULL::INT          AS vencimento_em_dias,
        saldo_nubank_deusa AS vlr_atualizado_brl,
        'BRL'              AS moeda_ativo,
        fl_mes_atual,
        'deusa'            AS pessoa
    FROM {{ ref('int_patrimonio_mensal_deusa') }}
),

ativos_cashback_lucas AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'NUBANK'                    AS instituicao,
        NULL::TEXT                  AS emissor,
        'NUBANK'                    AS conglomerado,
        'DISPONIBILIDADE'           AS classe_ativo,
        'CONTA CORRENTE'            AS tipo_ativo,
        'SALDO EM CONTA'            AS ativo,
        NULL::TEXT                  AS indexador,
        NULL::DATE                  AS data_vencimento,
        NULL::INT                   AS vencimento_em_dias,
        saldo_nubank_cashback_lucas AS vlr_atualizado_brl,
        'BRL'                       AS moeda_ativo,
        fl_mes_atual,
        'lucas'                     AS pessoa
    FROM {{ ref('int_patrimonio_mensal') }}
),

ativos_cashback_deusa AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'NUBANK'                    AS instituicao,
        NULL::TEXT                  AS emissor,
        'NUBANK'                    AS conglomerado,
        'DISPONIBILIDADE'           AS classe_ativo,
        'CONTA CORRENTE'            AS tipo_ativo,
        'SALDO EM CONTA'            AS ativo,
        NULL::TEXT                  AS indexador,
        NULL::DATE                  AS data_vencimento,
        NULL::INT                   AS vencimento_em_dias,
        saldo_nubank_cashback_deusa AS vlr_atualizado_brl,
        'BRL'                       AS moeda_ativo,
        fl_mes_atual,
        'deusa'                     AS pessoa
    FROM {{ ref('int_patrimonio_mensal_deusa') }}
),

ativos_wise_lucas AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'WISE'            AS instituicao,
        NULL::TEXT        AS emissor,
        NULL::TEXT        AS conglomerado,
        'DISPONIBILIDADE' AS classe_ativo,
        'CONTA CORRENTE'  AS tipo_ativo,
        'SALDO EM CONTA'  AS ativo,
        NULL::TEXT        AS indexador,
        NULL::DATE        AS data_vencimento,
        NULL::INT         AS vencimento_em_dias,
        saldo_wise_lucas  AS vlr_atualizado_brl,
        'USD'             AS moeda_ativo,
        fl_mes_atual,
        'lucas'           AS pessoa
    FROM {{ ref('int_patrimonio_mensal') }}
),

ativos_bitcoin_lucas AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'AUTOCUSTODIA'      AS instituicao,
        NULL::TEXT          AS emissor,
        NULL::TEXT          AS conglomerado,
        'DISPONIBILIDADE'   AS classe_ativo,
        'CONTA CORRENTE'    AS tipo_ativo,
        'SALDO EM CONTA'    AS ativo,
        NULL::TEXT          AS indexador,
        NULL::DATE          AS data_vencimento,
        NULL::INT           AS vencimento_em_dias,
        saldo_bitcoin_lucas AS vlr_atualizado_brl,
        'BTC'               AS moeda_ativo,
        fl_mes_atual,
        'lucas'             AS pessoa
    FROM {{ ref('int_patrimonio_mensal') }}
),

ativos_bb_jessica AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'BANCO DO BRASIL'          AS instituicao,
        NULL::TEXT                 AS emissor,
        'BANCO DO BRASIL'          AS conglomerado,
        'DISPONIBILIDADE'          AS classe_ativo,
        'CONTA CORRENTE'           AS tipo_ativo,
        'SALDO EM CONTA'           AS ativo,
        'TIR'                      AS indexador,
        NULL::DATE                 AS data_vencimento,
        NULL::INT                  AS vencimento_em_dias,
        saldo_banco_brasil_jessica AS vlr_atualizado_brl,
        'BRL'                      AS moeda_ativo,
        fl_mes_atual,
        'jessica'                  AS pessoa
    FROM {{ ref('int_patrimonio_mensal') }}
),

ativos_bb_deusa AS (
    SELECT
        mes_base,
        mes_final,
        trimestre,
        ano,
        'BANCO DO BRASIL'        AS instituicao,
        NULL::TEXT               AS emissor,
        'BANCO DO BRASIL'        AS conglomerado,
        'DISPONIBILIDADE'        AS classe_ativo,
        'CONTA CORRENTE'         AS tipo_ativo,
        'SALDO EM CONTA'         AS ativo,
        'TIR'                    AS indexador,
        NULL::DATE               AS data_vencimento,
        NULL::INT                AS vencimento_em_dias,
        saldo_banco_brasil_deusa AS vlr_atualizado_brl,
        'BRL'                    AS moeda_ativo,
        fl_mes_atual,
        'deusa'                  AS pessoa
    FROM {{ ref('int_patrimonio_mensal_deusa') }}
),

unioned AS (
    SELECT * FROM ativos_bradesco_lucas
    UNION ALL
    SELECT * FROM ativos_bradesco_deusa
    UNION ALL
    SELECT * FROM ativos_nubank_deusa
    UNION ALL
    SELECT * FROM ativos_cashback_lucas
    UNION ALL
    SELECT * FROM ativos_cashback_deusa
    UNION ALL
    SELECT * FROM ativos_wise_lucas
    UNION ALL
    SELECT * FROM ativos_bitcoin_lucas
    UNION ALL
    SELECT * FROM ativos_bb_jessica
    UNION ALL
    SELECT * FROM ativos_bb_deusa
),



{#- A ordem das colunas abaixo é idêntica à de int_carteira. Os marts que unem os
    dois (carteira_lucas, carteira_jessica, carteira_deusa, carteira_classificacao)
    projetam colunas nomeadas, então a igualdade de ordem não é mais obrigatória —
    mas mantê-la deixa os dois modelos comparáveis lado a lado. -#}
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
        COALESCE(cls.camada, 'NAO CLASSIFICADO') AS camada,
        t1.ativo,
        COALESCE(t1.indexador, cls.indexador)    AS indexador,
        t1.data_vencimento,
        t1.vencimento_em_dias,
        t1.vlr_atualizado_brl,
        t1.moeda_ativo,
        t1.fl_mes_atual,
        t1.pessoa
    FROM unioned AS t1
    LEFT JOIN LATERAL (
        SELECT
            c.camada,
            c.indexador
        FROM {{ ref('stg_carteira_classificacao') }} AS c
        WHERE
            c.pessoa = t1.pessoa
            AND c.ativo = t1.ativo
            AND c.classe_ativo = t1.classe_ativo
            AND c.tipo_ativo = t1.tipo_ativo
            AND c.instituicao = t1.instituicao
            AND c.mes_base <= t1.mes_base
        ORDER BY c.mes_base DESC
        LIMIT 1
    ) AS cls ON TRUE --noqa
)

SELECT * FROM final
ORDER BY mes_base, pessoa, instituicao, classe_ativo, tipo_ativo, ativo
