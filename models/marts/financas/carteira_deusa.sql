{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

SELECT
    *,
    CURRENT_TIMESTAMP AS model_updated_at
FROM {{ ref('int_carteira') }}
WHERE pessoa = 'deusa'
ORDER BY mes_base, pessoa, instituicao, classe_ativo, tipo_ativo, ativo
