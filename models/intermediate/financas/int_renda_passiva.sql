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
        {{ normaliza_instituicao('instituicao') }} AS instituicao,
        vlr_liquido_brl,
        moeda_ativo
    FROM unioned
)

SELECT * FROM final
ORDER BY mes_base
