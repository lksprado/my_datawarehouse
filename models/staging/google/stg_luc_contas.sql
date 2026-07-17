{{
  config(
    tags = ['investimentos', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw','luc_contas') }}
),

renamed AS (
    SELECT
        TO_DATE(fatura, 'MM-YYYY')     AS mes_fatura,
        TO_DATE(mes, 'MM-YYYY')        AS mes_debito,
        TO_DATE(data, 'dd/MM/yyyy')    AS data_debito,
        {{ clean_string("dia","upper") }} AS nome_dia,
        dia_ajustado,
        dia_real,
        REPLACE(
            REPLACE(REGEXP_REPLACE(t_mercado, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2)                     AS mercado,

        REPLACE(
            REPLACE(REGEXP_REPLACE(t_diversos, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2)                     AS diversos,

        REPLACE(
            REPLACE(REGEXP_REPLACE(t_assinaturas, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2)                     AS assinaturas,

        REPLACE(
            REPLACE(REGEXP_REPLACE(t_role, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2)                     AS role,

        REPLACE(
            REPLACE(REGEXP_REPLACE(t_transporte, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2)                     AS transporte,

        REPLACE(
            REPLACE(REGEXP_REPLACE(t_apartamento, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2)                     AS apartamento,

        REPLACE(
            REPLACE(REGEXP_REPLACE(t_saude, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2)                     AS saude,

        REPLACE(
            REPLACE(REGEXP_REPLACE(t_educacao, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2)                     AS educacao,
        'lucas'                          AS pessoa
    FROM source
)

SELECT * FROM renamed
