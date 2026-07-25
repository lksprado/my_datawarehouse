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
        market_value * vlr_usd AS vlr_atualizado_brl,
        moeda_ativo
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
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        emissor,
                        '\s+S(?:\.|/)?A\.?\s*$',
                        '',
                        'i'
                    ),
                    '[[:punct:]]+',
                    ' ',
                    'g'
                ),
                '\s+',
                ' ',
                'g'
            )
        ) as emissor,                   
        'TITULO PRIVADO'                                                                                                        AS tipo_investimento,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        produto,
                        '\s+S(?:\.|/)?A\.?\s*$',
                        '',
                        'i'
                    ),
                    '[[:punct:]]+',
                    ' ',
                    'g'
                ),
                '\s+',
                ' ',
                'g'
            )
        ) || ' - ' || EXTRACT(YEAR FROM data_vencimento) AS investimento,
        indexador,
        data_emissao,
        data_vencimento,
        COALESCE(vlr_atualizado_curva, vlr_atualizado_mtm)                                                                      AS vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_renda_fixa') }}
),

td AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        'TESOURO NACIONAL'::TEXT       AS emissor,
        'TITULO PUBLICO' AS tipo_investimento,
        TRIM(produto)    AS investimento,
        indexador,
        NULL::DATE       AS data_emissao,
        data_vencimento,
        vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_tesouro_direto') }}
),

faltantes_lucas AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        emissor,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_investimentos_faltantes_lucas') }}
),

faltantes_jessica AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        emissor,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_investimentos_faltantes_jessica') }}
),

faltantes_deusa AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        emissor,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        vlr_atualizado_brl,
        moeda_ativo
    FROM {{ ref('stg_investimentos_faltantes_deusa') }}
),

unioned AS (
    SELECT * FROM avenue
    UNION ALL
    SELECT * FROM rf
    UNION ALL
    SELECT * FROM td
    UNION ALL
    SELECT * FROM faltantes_lucas
    UNION ALL
    SELECT * FROM faltantes_jessica
    UNION ALL
    SELECT * FROM faltantes_deusa
),

treated AS (
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
        CASE
            WHEN emissor LIKE '%BANCO MAXIMA%' THEN 'BANCO MASTER'
            WHEN emissor LIKE '%BANCO MASTER%' THEN 'BANCO MASTER'
            WHEN emissor LIKE 'NU FINANCEIRA SA SOCIEDADE DE CREDITO FINANCIAMENTO E INVESTIMENTO' THEN 'NUBANK'
            WHEN emissor LIKE 'NU FINANCEIRA SA SOCIEDADE CFI' THEN 'NUBANK'
            ELSE emissor
        END AS emissor,
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
        moeda_ativo,
        SUM(vlr_atualizado_brl)::INT AS vlr_atualizado_brl        
    FROM unioned
    WHERE vlr_atualizado_brl IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),
final as (
    SELECT
        t1.mes_base,
        t1.pessoa,
        t1.instituicao,
        t1.emissor,
        t2.conglomerado,
        t1.categoria_investimento,
        t1.tipo_investimento,
        t1.investimento,
        t1.indexador,
        t1.data_emissao,
        t1.data_vencimento,
        t1.vencimento_em_dias,
        t1.vlr_atualizado_brl,
        t1.moeda_ativo
    FROM treated t1
    LEFT JOIN {{ ref('stg_de_para_instituicoes_fgc') }} t2
    ON t1.emissor = t2.instituicao
)

SELECT * FROM final
ORDER BY mes_base
