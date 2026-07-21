{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

WITH
carteira as (
    SELECT
    mes_base,
    conglomerado,
    250000 as limite_fgc,
    SUM(vlr_atualizado) as total
    FROM {{ ref('carteira_conjunta') }}
    WHERE 1=1
    AND pessoa = 'jessica'
    AND fl_mes_atual IS TRUE
    AND conglomerado IS NOT NULL
    GROUP BY 1,2,3
),
calc AS (
    SELECT
    mes_base,
    conglomerado,
    limite_fgc-total AS vlr_liberado,
    CASE
        WHEN limite_fgc-total > 1000 THEN 'OK'
        ELSE 'PERIGO!'
    END AS risco_fgc
    FROM carteira
)
SELECT * FROM calc