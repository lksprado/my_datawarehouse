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
    'FUNDO' as tipo_investimento,
    symbol_cusip as ticker,
    (market_value::INT * vlr_usd)::INT as vlr_atualizado
    from {{ ref('stg_avenue')}}
    inner join {{ ref('stg_usd') }}
    on period_end = data_referencia
    where (asset_class = 'EQUITIES' OR asset_class = 'CASH')
    and symbol_cusip NOT IN ('GOVT','TFLO')
),
acoes as (
    select
    mes_base,
    pessoa,
    instituicao,
    'ACAO' as tipo_investimento,
    ticker,
    vlr_atualizado
    from {{ ref('stg_acoes') }}
),
bdr as (
    select
    mes_base,
    pessoa,
    instituicao,
    'ACAO' as tipo_investimento,
    ticker,
    vlr_atualizado
    from {{ ref('stg_acoes') }}
),
etf as (
    select 
    mes_base,
    pessoa,
    instituicao,
    'FUNDO' as tipo_investimento,
    ticker,
    vlr_atualizado
    from {{ ref('stg_etf') }}
),
fundos as (
    select 
    mes_base,
    pessoa,
    instituicao,
    'FUNDO' as tipo_investimento,
    ticker,
    vlr_atualizado
    from {{ ref('stg_fundos') }}
),
unioned as (
    select * from avenue
    union all
    select * from acoes
    union all 
    select * from etf
    union all 
    select * from fundos
    union all 
    select * from bdr
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
            ELSE 'DESCONHECIDO'
        END AS instituicao,
        'RENDA VARIAVEL' as categoria_investimento,
        tipo_investimento,
        ticker as investimento,
        sum(vlr_atualizado)::int as vlr_atualizado
    from unioned
    group by 1,2,3,4,5,6
)
select * from final order by mes_base