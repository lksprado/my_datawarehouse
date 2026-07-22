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
    FROM {{ source('raw' ,'etf') }}
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
        )                                                                           AS mes_base,
        pessoa,
        {{ clean_string("produto", "upper") }}                                      AS produto,
        {{ clean_string("instituicao", "upper") }}                                  AS instituicao,
        codigo_de_negociacao                                                        AS ticker,
        NULLIF(REGEXP_REPLACE(cnpj_do_fundo, '[^0-9]', '', 'g'), '')::BIGINT        AS cnpj_empresa,
        {{ clean_string("tipo", "upper") }}                                         AS tipo,
        quantidade::INT                                                             AS quantidade,
        NULLIF(REGEXP_REPLACE(quantidade_disponivel, '[^0-9]', '', 'g'), '')::INT   AS quantidade_disponivel,
        NULLIF(REGEXP_REPLACE(quantidade_indisponivel, '[^0-9]', '', 'g'), '')::INT AS quantidade_indisponivel,
        NULLIF(motivo, '-')                                                         AS motivo_indisponibilidade,
        preco_de_fechamento                                                         AS vlr_fechamento,
        valor_atualizado                                                            AS vlr_atualizado_brl,
        'BRL'                                                                       AS moeda_ativo
    FROM source
)

SELECT * FROM renamed
