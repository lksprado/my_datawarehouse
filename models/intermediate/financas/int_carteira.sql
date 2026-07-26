{{
  config(
    materialized = 'table',
    tags = ['financas', 'intermediate'],
  )
}}

{#- Materializado como table (foge do default view do intermediate) porque é
    consumido por 9 marts a jusante (carteira_lucas_jessica, carteira_deusa,
    carteira_classificacao, risco_fgc_*, carteira_*_agregada); como view, o
    union + join as-of LATERAL seria recalculado do zero em cada um deles. -#}

WITH
datas AS (
    SELECT DISTINCT
        month_start_date,
        month_end_date,
        quarter_of_year,
        year_number
    FROM {{ ref('dim_datas') }}
),

variavel AS (
    SELECT
        instituicao,
        NULL::TEXT AS emissor,
        NULL::TEXT AS conglomerado,
        classe_ativo,
        tipo_ativo,
        ativo,
        NULL::TEXT AS indexador,
        NULL::DATE AS data_vencimento,
        NULL::INT AS vencimento_em_dias,
        vlr_atualizado_brl,
        moeda_ativo,
        mes_base,
        pessoa
    FROM {{ ref('int_renda_variavel') }}
),

fixa AS (
    SELECT
        instituicao,
        emissor,
        conglomerado,
        classe_ativo,
        tipo_ativo,
        ativo,
        indexador,
        data_vencimento,
        vencimento_em_dias,
        vlr_atualizado_brl,
        moeda_ativo,
        mes_base,
        pessoa
    FROM {{ ref('int_renda_fixa') }}
),

unioned AS (
    SELECT * FROM variavel
    UNION ALL
    SELECT * FROM fixa
),
mes_base_mais_recente AS (
    SELECT
    MAX(mes_base) as mes_base_mais_recente
    FROM unioned
),
final AS (
    SELECT
        t1.mes_base,
        t2.month_end_date   AS mes_final,
        t2.quarter_of_year  AS trimestre,
        t2.year_number      AS ano,
        t1.instituicao,
        t1.emissor,
        t1.conglomerado,
        t1.classe_ativo,
        t1.tipo_ativo,
        COALESCE(cls.camada, 'NAO CLASSIFICADO')             AS camada,
        t1.ativo,
        COALESCE(t1.indexador, cls.indexador)                AS indexador,
        t1.data_vencimento,
        t1.vencimento_em_dias,
        t1.vlr_atualizado_brl,
        t1.moeda_ativo,
        
        CASE
            WHEN t3.mes_base_mais_recente IS NULL THEN false
            ELSE true
        END AS fl_mes_atual,
        t1.pessoa
    FROM unioned AS t1
    INNER JOIN datas AS t2
        ON t1.mes_base = t2.month_start_date
    LEFT JOIN mes_base_mais_recente t3
        ON t1.mes_base = t3.mes_base_mais_recente
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
