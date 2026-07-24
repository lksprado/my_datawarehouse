{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

SELECT * FROM {{ ref('int_carteira') }}
WHERE pessoa IN ('lucas', 'jessica')
ORDER BY mes_base, pessoa, instituicao, categoria_investimento, tipo_investimento, investimento
