{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

WITH
source AS (
    SELECT
        *,
        SPLIT_PART(source_path, '/', -2) AS mes_base,
        SPLIT_PART(source_path, '/', -3) AS pessoa
    FROM {{ source('raw' ,'tesouro_direto') }}
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
        )                                          AS mes_base,
        pessoa,
        {{ clean_string("produto", "upper") }}     AS produto,
        {{ clean_string("instituicao", "upper") }} AS instituicao,
        codigo_isin,
        NULLIF(indexador, '-')                     AS indexador,
        TO_DATE(vencimento, 'dd/MM/yyyy')          AS data_vencimento,
        quantidade,
        quantidade_disponivel,
        quantidade_indisponivel,
        NULLIF(motivo, '-')                        AS motivo_indisponibilidade,
        valor_aplicado::NUMERIC(18, 2)              AS vlr_aplicado,
        valor_bruto::NUMERIC(18, 2)                 AS vlr_bruto,
        valor_liquido::NUMERIC(18, 2)               AS vlr_liquido,
        valor_atualizado::NUMERIC(18, 2)            AS vlr_atualizado
    FROM source
)

SELECT * FROM renamed
