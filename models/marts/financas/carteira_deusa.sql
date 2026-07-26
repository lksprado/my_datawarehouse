{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

SELECT * FROM {{ ref('int_carteira') }}
WHERE pessoa = 'deusa'
ORDER BY mes_base, pessoa, instituicao, classe_ativo, tipo_ativo, ativo
