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
    FROM {{ source('raw' ,'renda_fixa') }}
    UNION ALL
    SELECT
        SPLIT_PART(source_path, '/', -2) AS mes_base,
        SPLIT_PART(source_path, '/', -3) AS pessoa,
        produto,
        instituicao,
        emissor,
        codigo,
        indexador,
        tipo_de_regime,
        data_de_emissao,
        vencimento,
        quantidade,
        quantidade_disponivel,
        quantidade_indisponivel,
        motivo,
        contraparte,
        preco_atualizado_mtm,
        valor_atualizado_mtm,
        preco_atualizado_curva::TEXT     AS preco_atualizado_curva,
        valor_atualizado_curva::TEXT     AS valor_atualizado_curva,
        source_path,
        "unnamed:_17",
        "unnamed:_18"
    FROM {{ ref('daycoval_backfill') }}
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
        )                                                                                      AS mes_base,
        pessoa,
        {{ clean_string("produto", "upper") }}                                                 AS produto,
        {{ clean_string("instituicao", "upper") }}                                             AS instituicao,
        {{ clean_string("emissor", "upper") }}                                                 AS emissor,
        codigo,
        NULLIF({{ clean_string("indexador", "upper") }}, '-')                                 AS indexador,
        {{ clean_string("tipo_de_regime", "upper") }}                                          AS tipo_de_regime,
        TO_DATE(data_de_emissao, 'dd/MM/yyyy')                                                 AS data_emissao,
        TO_DATE(vencimento, 'dd/MM/yyyy')                                                      AS data_vencimento,
        quantidade::INT                                                                        AS quantidade,
        quantidade_disponivel::INT                                                             AS quantidade_disponivel,
        NULLIF(REGEXP_REPLACE(quantidade_indisponivel, '[^0-9]', '', 'g'), '')::NUMERIC(18, 2) AS quantidade_indisponivel,
        NULLIF(motivo, '-')                                                                    AS motivo_indisponibilidade,
        NULLIF(contraparte, '-')                                                               AS contraparte,
        NULLIF(preco_atualizado_mtm, '-')::NUMERIC(18, 2)                                      AS preco_atualizado_mtm,
        NULLIF(valor_atualizado_mtm, '-')::NUMERIC(18, 2)                                      AS vlr_atualizado_mtm,
        NULLIF(preco_atualizado_curva, '-')::NUMERIC(18, 9)                                    AS preco_atualizado_curva,
        NULLIF(valor_atualizado_curva, '-')::NUMERIC(18, 2)                                    AS vlr_atualizado_curva
    FROM source
)

SELECT * FROM renamed
