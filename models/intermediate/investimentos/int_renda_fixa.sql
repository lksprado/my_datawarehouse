{{
  config(
    tags = ['investimentos', 'staging'],
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
        period_start           AS mes_base,
        pessoa,
        'AVENUE'               AS instituicao,
        'TITULO PUBLICO'       AS tipo_investimento,
        description            AS investimento,
        NULL::TEXT             AS indexador,
        NULL::DATE             AS data_emissao,
        NULL::DATE             AS data_vencimento,
        market_value * vlr_usd AS vlr_atualizado
    FROM {{ ref('stg_avenue') }}
    INNER JOIN {{ ref('stg_usd') }}
        ON period_end = data_referencia
    WHERE asset_class = 'FIXED INCOME' AND pessoa <> 'lucas'
),

rf AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'TITULO PRIVADO'                                                                                      AS tipo_investimento,
        REPLACE(REPLACE(TRIM(produto), 'S/A', ''), 'S.A.', '') || ' - ' || EXTRACT(YEAR FROM data_vencimento) AS investimento,
        indexador,
        data_emissao,
        data_vencimento,
        COALESCE(vlr_atualizado_curva, vlr_atualizado_mtm)                                                    AS vlr_atualizado
    FROM {{ ref('stg_renda_fixa') }}
),

td AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'TITULO PUBLICO' AS tipo_investimento,
        TRIM(produto)    AS investimento,
        indexador,
        NULL::DATE       AS data_emissao,
        data_vencimento,
        vlr_atualizado
    FROM {{ ref('stg_tesouro_direto') }}
),

faltantes AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        vlr_atualizado
    FROM {{ ref('stg_investimentos_faltantes') }}
),

unioned AS (
    SELECT * FROM avenue
    UNION ALL
    SELECT * FROM rf
    UNION ALL
    SELECT * FROM td
    UNION ALL
    SELECT * FROM faltantes
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
            ELSE 'DESCONHECIDO'
        END                      AS instituicao,
        'RENDA FIXA'             AS categoria_investimento,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        SUM(vlr_atualizado)::INT AS vlr_atualizado
    FROM unioned
    WHERE vlr_atualizado IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)

SELECT * FROM final
ORDER BY mes_base
