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
        period_start           AS mes_base,
        pessoa,
        'AVENUE'               AS instituicao,
        NULL::TEXT             AS emissor,
        'TITULO PUBLICO'       AS tipo_investimento,
        description            AS investimento,
        NULL::TEXT             AS indexador,
        NULL::DATE             AS data_emissao,
        NULL::DATE             AS data_vencimento,
        market_value * vlr_usd AS vlr_atualizado
    FROM {{ ref('stg_assets') }}
    INNER JOIN {{ ref('stg_usd') }}
        ON period_end = data_referencia
    WHERE asset_class = 'FIXED INCOME' AND pessoa <> 'lucas'
),

rf AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        TRIM(REPLACE(REPLACE(REPLACE(emissor, 'S/A', ''), 'S.A.', ''),'S.A',''))                                                AS emissor,                       
        'TITULO PRIVADO'                                                                                                        AS tipo_investimento,
        TRIM(REPLACE(REPLACE(REPLACE(produto, 'S/A', ''), 'S.A.', ''),'S.A','')) || ' - ' || EXTRACT(YEAR FROM data_vencimento) AS investimento,
        indexador,
        data_emissao,
        data_vencimento,
        COALESCE(vlr_atualizado_curva, vlr_atualizado_mtm)                                                                      AS vlr_atualizado
    FROM {{ ref('stg_renda_fixa') }}
),

td AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        NULL::TEXT       AS emissor,
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
        NULL::TEXT AS emissor,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        vlr_atualizado
    FROM {{ ref('stg_investimentos_faltantes') }}
),
faltantes_deusa AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        NULL::TEXT AS emissor,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        vlr_atualizado
    FROM {{ ref('stg_investimentos_faltantes_deusa') }}
),

unioned AS (
    SELECT * FROM avenue
    UNION ALL
    SELECT * FROM rf
    UNION ALL
    SELECT * FROM td
    UNION ALL
    SELECT * FROM faltantes
    UNION ALL
    SELECT * FROM faltantes_deusa
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
        emissor,
        'RENDA FIXA'             AS categoria_investimento,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        CASE 
            WHEN data_vencimento >= CURRENT_DATE THEN data_vencimento-CURRENT_DATE
            ELSE NULL
        END                      AS vencimento_em_dias,
        SUM(vlr_atualizado)::INT AS vlr_atualizado
    FROM unioned
    WHERE vlr_atualizado IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

SELECT * FROM final
ORDER BY mes_base
