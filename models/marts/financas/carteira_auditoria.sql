{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}


WITH
carteira_lucas AS (
    SELECT
        mes_base,
        pessoa,
        SUM(vlr_atualizado_brl) AS total_carteira
    FROM {{ ref('carteira_lucas') }}
    GROUP BY
        mes_base,
        pessoa
    ORDER BY
        mes_base
),

carteira_deusa AS (
    SELECT
        mes_base,
        pessoa,
        SUM(vlr_atualizado_brl) AS total_carteira
    FROM {{ ref('carteira_deusa') }}
    GROUP BY
        mes_base,
        pessoa
    ORDER BY
        mes_base
),

carteira_jessica AS (
    SELECT
        mes_base,
        pessoa,
        SUM(vlr_atualizado_brl) AS total_carteira
    FROM {{ ref('carteira_jessica') }}
    GROUP BY
        mes_base,
        pessoa
    ORDER BY
        mes_base
),

carteira_agregada_lucas AS (
    SELECT
        mes_base,
        pessoa,
        SUM(total_geral) AS total_carteira_agregada
    FROM {{ ref('carteira_lucas_agregada') }}
    GROUP BY
        mes_base,
        pessoa
    ORDER BY
        mes_base
),

carteira_agregada_deusa AS (
    SELECT
        mes_base,
        pessoa,
        SUM(total_geral) AS total_carteira_agregada
    FROM {{ ref('carteira_deusa_agregada') }}
    GROUP BY
        mes_base,
        pessoa
    ORDER BY
        mes_base
),

carteira_agregada_jessica AS (
    SELECT
        mes_base,
        pessoa,
        SUM(total_geral) AS total_carteira_agregada
    FROM {{ ref('carteira_jessica_agregada') }}
    GROUP BY
        mes_base,
        pessoa
    ORDER BY
        mes_base
),

unioned_carteiras AS (
    SELECT * FROM carteira_lucas
    UNION ALL
    SELECT * FROM carteira_deusa
    UNION ALL
    SELECT * FROM carteira_jessica
),

unioned_carteiras_agregada AS (
    SELECT * FROM carteira_agregada_lucas
    UNION ALL
    SELECT * FROM carteira_agregada_deusa
    UNION ALL
    SELECT * FROM carteira_agregada_jessica
),

final AS (
    SELECT
        uc.mes_base,
        uc.pessoa,
        uc.total_carteira,
        uca.total_carteira_agregada,
        uc.total_carteira - uca.total_carteira_agregada AS dif
    FROM unioned_carteiras AS uc
    INNER JOIN unioned_carteiras_agregada AS uca
        ON uc.mes_base = uca.mes_base
        AND uc.pessoa = uca.pessoa
    ORDER BY
        uc.mes_base,
        uc.pessoa
)

SELECT * FROM final
