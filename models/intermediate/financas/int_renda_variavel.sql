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
        period_start                       AS mes_base,
        pessoa,
        'AVENUE'                           AS instituicao,
        'FUNDO'                            AS tipo_investimento,
        symbol_cusip                       AS ticker,
        (market_value::INT * vlr_usd)::INT AS vlr_atualizado
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
        'ACAO' AS tipo_investimento,
        ticker,
        vlr_atualizado
    FROM {{ ref('stg_acoes') }}
),

bdr AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'ACAO' AS tipo_investimento,
        ticker,
        vlr_atualizado
    FROM {{ ref('stg_acoes') }}
),

etf AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'FUNDO' AS tipo_investimento,
        ticker,
        vlr_atualizado
    FROM {{ ref('stg_etf') }}
),

fundos AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'FUNDO' AS tipo_investimento,
        ticker,
        vlr_atualizado
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
        CASE
            WHEN instituicao = 'BANCO DAYCOVAL S/A' THEN 'DAYCOVAL'
            WHEN instituicao = 'NU INVESTIMENTOS S.A. - CTVM' THEN 'NUBANK'
            WHEN instituicao = 'EASYNVEST - TITULO CV S/A' THEN 'NUBANK'
            WHEN instituicao = 'NU INVEST CORRETORA DE VALORES S.A.' THEN 'NUBANK'
            WHEN instituicao = 'BANCO BRADESCO S/A' THEN 'BRADESCO'
            WHEN instituicao = 'ITAU UNIBANCO S.A.' THEN 'ITAU'
            WHEN instituicao = 'BANCO SOFISA S/A' THEN 'SOFISA'
            WHEN instituicao = 'AVENUE' THEN 'AVENUE'
            WHEN instituicao = 'NUBANK' THEN 'NUBANK'
            WHEN instituicao = 'ITAU' THEN 'ITAU'
            WHEN instituicao = 'SOFISA' THEN 'SOFISA'
            WHEN instituicao = 'BANCO DO BRASIL S/A' THEN 'BANCO DO BRASIL'
            ELSE 'DESCONHECIDO'
        END                      AS instituicao,
        'RENDA VARIAVEL'         AS categoria_investimento,
        tipo_investimento,
        ticker                   AS investimento,
        SUM(vlr_atualizado)::INT AS vlr_atualizado
    FROM unioned
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT * FROM final
ORDER BY mes_base
