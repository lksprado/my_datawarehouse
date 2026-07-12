{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

WITH
source AS (
    SELECT
        SPLIT_PART(source_path, '/', -2) AS mes_base,
        SPLIT_PART(source_path, '/', -3) AS pessoa,
        *
    FROM {{ source('raw' ,'proventos') }}
),

renamed AS (
    SELECT
        MAKE_DATE(
            SPLIT_PART(mes_base, '-', 1)::INT,
            ARRAY_POSITION(
                ARRAY[
                    'janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho',
                    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
                ],
                {{ clean_string("split_part(mes_base, '-', 2)", "lower") }}
            ),
            1
        )                                             AS mes_base,
        pessoa,
        TO_DATE(pagamento, 'dd/MM/yyyy')              AS data_pagamento,
        {{ clean_string("produto", "upper") }}        AS produto,
        {{ clean_string("instituicao", "upper") }}    AS instituicao,
        {{ clean_string("tipo_de_evento", "upper") }} AS tipo_provento,
        quantidade::INT                               AS quantidade,
        preco_unitario,
        valor_liquido                                 AS vlr_liquido
    FROM source
)

SELECT * FROM renamed
