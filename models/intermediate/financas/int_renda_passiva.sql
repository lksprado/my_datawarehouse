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
        moeda_ativo,
        (vlr_liquido_usd*vlr_usd)::INT     AS vlr_liquido_brl        
    FROM {{ ref('stg_dividends_interest') }}
    INNER JOIN {{ ref('stg_usd') }}
        ON period_end = data_referencia
),
b3 AS (
    SELECT
        mes_base,
        pessoa,
        instituicao,
        moeda_ativo,
        SUM(vlr_liquido_brl)::INT                   AS vlr_liquido_brl        
    FROM {{ ref('stg_proventos') }}
    WHERE tipo_provento <> 'PAGAMENTO DE JUROS'
    GROUP BY 
    1,2,3,4
),

unioned AS (
    SELECT * FROM avenue
    UNION ALL
    SELECT * FROM b3
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
        'RENDA PASSIVA'          AS categoria_investimento,
        vlr_liquido_brl,
        moeda_ativo
    FROM unioned
)

SELECT * FROM final
ORDER BY mes_base
