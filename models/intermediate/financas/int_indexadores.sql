{{
  config(
    tags = ['financas', 'intermediate'],
  )
}}

WITH 
ativos as (
    SELECT 
        mes,
        minha_inflacao,
        ipca,
        igpm,
        selic,
        cdi,
        minha_inflacao_acum,
        ipca_acum,
        igpm_acum,
        selic_acum,
        cdi_acum
    FROM {{ ref ('stg_ativos') }}
)
select * from ativos