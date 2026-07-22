{{
  config(
    materialized = 'table',
    tags = ['financas', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw','carteira_classificacao') }}
),

renamed AS (
    SELECT
        pessoa,
        investimento,
        categoria_investimento,
        tipo_investimento,
        instituicao,
        camada,
        TO_DATE(mes_base, 'YYYY-MM-DD') AS mes_base
    FROM source
)

SELECT * FROM renamed
