{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

with 
variavel as (
    select 
    (date_trunc('month', mes_base) + interval '1 month - 1 day')::date AS mes,
    instituicao,
    categoria_investimento,
    tipo_investimento,
    NULL::TEXT AS camada,
    investimento,
    NULL::TEXT as indexador,
    NULL::DATE as data_vencimento,
    vlr_atualizado,
    mes_base,
    pessoa
     from {{ ref('int_renda_variavel') }}
),
fixa as (
    select
    (date_trunc('month', mes_base) + interval '1 month - 1 day')::date AS mes,
    instituicao,
    categoria_investimento,
    tipo_investimento,
    NULL::TEXT AS camada,
    investimento,
    indexador,
    data_vencimento,
    vlr_atualizado,
    mes_base,
    pessoa
     from {{ ref('int_renda_fixa') }}
),
unioned as (
    select * from variavel 
    union all 
    select * from fixa 
)
select * from unioned order by mes_base, pessoa, instituicao, categoria_investimento, tipo_investimento, investimento
