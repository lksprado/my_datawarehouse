{{
  config(
    tags = ['financas', 'intermediate'],
  )
}}

WITH
usd AS (
    SELECT
        data_referencia,
        vlr_usd
    FROM {{ ref('stg_usd') }}
),

avenue AS (
    SELECT
        period_start                                         AS mes_base,
        pessoa,
        'AVENUE'                                             AS instituicao,
        CASE 
            WHEN symbol_cusip = 'CASH' 
                THEN 'CONTA CORRENTE'
        ELSE 'FUNDO' END                                     AS tipo_ativo,
        CASE 
            WHEN symbol_cusip = 'CASH' 
                THEN 'SALDO EM CONTA'
        ELSE symbol_cusip END                                AS ticker,
        (market_value::INT * vlr_usd)::INT                   AS vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_assets') }}
    INNER JOIN {{ ref('stg_usd') }}
        ON period_end = data_referencia
    WHERE
        (asset_class = 'EQUITIES' OR asset_class = 'CASH')
        AND symbol_cusip NOT IN ('GOVT', 'TFLO')
),

acoes AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'ACAO' AS tipo_ativo,
        ticker,
        vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_acoes') }}
),

bdr AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'ACAO' AS tipo_ativo,
        ticker,
        vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_bdr') }}
),

etf AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'FUNDO' AS tipo_ativo,
        ticker,
        vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_etf') }}
),

fundos AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'FUNDO' AS tipo_ativo,
        ticker,
        vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_fundos') }}
),

unioned AS (
    SELECT * FROM avenue
    UNION ALL
    SELECT * FROM acoes
    UNION ALL
    SELECT * FROM etf
    UNION ALL
    SELECT * FROM fundos
    UNION ALL
    SELECT * FROM bdr
),

final AS (
    SELECT
        mes_base,
        pessoa,
        {{ normaliza_instituicao('instituicao') }} AS instituicao,
        CASE
            WHEN tipo_ativo = 'CONTA CORRENTE'
                THEN 'DISPONIBILIDADE'
        ELSE 'RENDA VARIAVEL' END              AS classe_ativo,
        tipo_ativo,
        ticker                                 AS ativo,
        SUM(vlr_atualizado_brl)::INT           AS vlr_atualizado_brl,
        moeda_ativo
    FROM unioned
    GROUP BY 1, 2, 3, 4, 5, 6, 8
)

SELECT * FROM final
ORDER BY mes_base
