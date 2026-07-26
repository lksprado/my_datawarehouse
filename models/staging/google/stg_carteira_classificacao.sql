{{
  config(
    materialized = 'table',
    tags = ['financas', 'staging'],
  )
}}

{#- A aba "classificacao" da planilha ainda usa os cabeçalhos antigos
    (investimento / categoria_investimento / tipo_investimento). O apelido abaixo
    traduz para o vocabulário de ativo. Quando a aba for renomeada, basta trocar
    o lado esquerdo dos três apelidos pelos nomes novos. -#}

WITH
source AS (
    SELECT * FROM {{ source('raw','carteira_classificacao') }}
),

renamed AS (
    SELECT
        pessoa,
        ativo,
        indexador,
        classe_ativo,
        tipo_ativo,
        instituicao,
        camada,
        TO_DATE(mes_base, 'YYYY-MM-DD') AS mes_base
    FROM source
)

SELECT * FROM renamed
