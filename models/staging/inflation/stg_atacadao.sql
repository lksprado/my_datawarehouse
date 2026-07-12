{{
  config(
    materialized = 'table',
    tags = ['inflacao', 'staging'],
  )
}}

WITH
source_1 AS (
    SELECT * FROM {{ ref('stg_atacadao_historico') }}
),

source_2 AS (
    SELECT * FROM {{ ref('stg_atacadao_novo') }}
),

unioned AS (
    SELECT * FROM source_1
    UNION ALL
    SELECT * FROM source_2
),

renamed AS (
    SELECT
        *,
        -- pega só as letras: "300g" -> "g", "500ml" -> "ml", "1,5l" -> "l"
        REGEXP_REPLACE(LOWER(product_unity), '[0-9.,\s]', '', 'g') AS unit_raw,

        CASE
            WHEN REGEXP_REPLACE(LOWER(product_unity), '[0-9.,\s]', '', 'g') IN ('kg', 'quilo', 'quilos') THEN 'kg'
            WHEN REGEXP_REPLACE(LOWER(product_unity), '[0-9.,\s]', '', 'g') IN ('g', 'grama', 'gramas') THEN 'g'
            WHEN REGEXP_REPLACE(LOWER(product_unity), '[0-9.,\s]', '', 'g') IN ('ml') THEN 'ml'
            WHEN REGEXP_REPLACE(LOWER(product_unity), '[0-9.,\s]', '', 'g') IN ('l', 'litro', 'litros') THEN 'l'
            WHEN REGEXP_REPLACE(LOWER(product_unity), '[0-9.,\s]', '', 'g') IN ('un', 'uni', 'unid', 'unidade', 'unidades', 'rolos', 'dúzias', 'folhas') THEN 'un'
        END                                                        AS unit_normalized
    FROM unioned

)

SELECT
    *,
    CASE
        WHEN quantity_type = 'volume' AND unit_normalized = 'ml'
            THEN quantity_value_raw / 1000
        WHEN quantity_type = 'volume' AND unit_normalized = 'l'
            THEN quantity_value_raw
        WHEN quantity_type = 'weight' AND unit_normalized = 'g'
            THEN quantity_value_raw / 1000
        WHEN quantity_type = 'weight' AND unit_normalized = 'kg'
            THEN quantity_value_raw
        WHEN quantity_type = 'unit'
            THEN quantity_value_raw
    END
        AS quantity_value_normalized

FROM renamed
WHERE unit_normalized IS NOT NULL
ORDER BY created_at
