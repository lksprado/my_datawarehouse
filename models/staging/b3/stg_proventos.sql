{{
  config(
    materialized = 'table',
    tags = ['financas', 'staging'],
  )
}}

WITH
source AS (
    SELECT
        *,
        substring(source_path FROM '(\d{4}-[a-zçãáéíóú]+)(?=\.xlsx$)') AS mes_base,
        CASE 
            WHEN source_path LIKE '%deusa%' THEN 'deusa'
            WHEN source_path LIKE '%jessica%' THEN 'jessica'
            WHEN source_path LIKE '%lucas%' THEN 'lucas'
            ELSE 'desconhecido'
        END                              AS pessoa
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
        valor_liquido                                 AS vlr_liquido_brl,
        'BRL'                                         AS moeda_ativo
    FROM source
)

SELECT * FROM renamed
