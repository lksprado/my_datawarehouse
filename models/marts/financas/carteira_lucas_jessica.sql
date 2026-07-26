{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

WITH
unioned AS (
  SELECT * FROM {{ ref('int_carteira') }} 
  UNION ALL
  SELECT * FROM {{ ref('int_carteira_extra') }} 
)
select * from unioned WHERE pessoa IN ('lucas', 'jessica')
ORDER BY mes_base, pessoa, instituicao, classe_ativo, tipo_ativo, ativo
