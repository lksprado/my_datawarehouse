{{
  config(
    tags = ['investimentos', 'staging'],
  )
}}

WITH
usd as (
    select 
    data_referencia,
    vlr_usd
    from {{ ref('stg_usd')}}
),
avenue as (
    select 
        period_start as mes_base,
        pessoa,
        'AVENUE' as instituicao,
        'TITULO PUBLICO' as tipo_investimento,
        description as investimento,
        NULL::TEXT AS indexador,
        NULL::DATE AS data_emissao,
        NULL::DATE AS data_vencimento,
        market_value * vlr_usd as vlr_atualizado
    from {{ ref('stg_avenue')}}
    inner join {{ ref('stg_usd') }}
    on period_end = data_referencia
    where asset_class = 'FIXED INCOME' AND pessoa <> 'lucas'
),
rf as (
    select
    mes_base,
    pessoa,
    instituicao,
    'TITULO PRIVADO' as tipo_investimento,
    replace(replace(TRIM(produto),'S/A',''),'S.A.','') || ' - ' || EXTRACT(YEAR FROM data_vencimento)  as investimento,
    indexador,
    data_emissao,
    data_vencimento,
    COALESCE(vlr_atualizado_curva, vlr_atualizado_mtm) as vlr_atualizado
    from {{ ref('stg_renda_fixa') }}
),
td as (
    select 
    mes_base,
    pessoa,
    instituicao,
    'TITULO PUBLICO' as tipo_investimento,
    TRIM(produto) as investimento,
    indexador,
    NULL::DATE AS data_emissao,
    data_vencimento,
    vlr_atualizado
    from {{ ref('stg_tesouro_direto') }}
),
faltantes as (
    select
    mes_base,
    pessoa,
    instituicao,
    tipo_investimento,
    investimento,
    indexador,
    data_emissao,
    data_vencimento,
    vlr_atualizado
    from {{ ref('stg_investimentos_faltantes')}}
),
unioned as (
    select * from avenue
    union all 
    select * from rf
    union all 
    select * from td
    union all 
    select * from faltantes
),
final as (
    select
        mes_base,
        pessoa,
        CASE 
            WHEN instituicao = 'BANCO DAYCOVAL S/A' then 'DAYCOVAL'
            WHEN instituicao = 'NU INVESTIMENTOS S.A. - CTVM' then 'NUBANK'
            WHEN instituicao = 'EASYNVEST - TITULO CV S/A' then 'NUBANK'
            WHEN instituicao = 'NU INVEST CORRETORA DE VALORES S.A.' then 'NUBANK'
            WHEN instituicao = 'BANCO BRADESCO S/A' then 'BRADESCO'
            WHEN instituicao = 'ITAU UNIBANCO S.A.' then 'ITAU'
            WHEN instituicao = 'BANCO SOFISA S/A' then 'SOFISA'
            WHEN instituicao = 'AVENUE' then 'AVENUE'
            WHEN instituicao = 'NUBANK' then 'NUBANK'
            WHEN instituicao = 'ITAU' then 'ITAU'
            WHEN instituicao = 'SOFISA' then 'SOFISA'
            ELSE 'DESCONHECIDO'
        END AS instituicao,
        'RENDA FIXA' as categoria_investimento,
        tipo_investimento,
        investimento,
        indexador,
        data_emissao,
        data_vencimento,
        sum(vlr_atualizado)::int as vlr_atualizado
    from unioned
    where vlr_atualizado is not null
    group by 1,2,3,4,5,6,7,8,9
)
select * from final order by mes_base