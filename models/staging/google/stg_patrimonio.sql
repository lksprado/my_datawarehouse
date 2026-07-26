{{
  config(
    tags = ['financas', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw','patrimonio') }}
),

renamed AS (
    SELECT
        TO_DATE(
            CASE
                WHEN mes LIKE 'jan.%' THEN '01/' || RIGHT(mes, 2)
                WHEN mes LIKE 'fev.%' THEN '02/' || RIGHT(mes, 2)
                WHEN mes LIKE 'mar.%' THEN '03/' || RIGHT(mes, 2)
                WHEN mes LIKE 'abr.%' THEN '04/' || RIGHT(mes, 2)
                WHEN mes LIKE 'mai.%' THEN '05/' || RIGHT(mes, 2)
                WHEN mes LIKE 'jun.%' THEN '06/' || RIGHT(mes, 2)
                WHEN mes LIKE 'jul.%' THEN '07/' || RIGHT(mes, 2)
                WHEN mes LIKE 'ago.%' THEN '08/' || RIGHT(mes, 2)
                WHEN mes LIKE 'set.%' THEN '09/' || RIGHT(mes, 2)
                WHEN mes LIKE 'out.%' THEN '10/' || RIGHT(mes, 2)
                WHEN mes LIKE 'nov.%' THEN '11/' || RIGHT(mes, 2)
                WHEN mes LIKE 'dez.%' THEN '12/' || RIGHT(mes, 2)
            END, 'MM/YY'
        )                                                              AS mes_base,

        REGEXP_REPLACE(patrimonio_total, '[^0-9]', '', 'g')::INT       AS total_patrimonio_bruto,
        REGEXP_REPLACE(patrimonio_r$, '[^0-9]', '', 'g')::INT          AS total_patrimonio_liquido,
        REGEXP_REPLACE(patrimonio_lucas_r$, '[^0-9]', '', 'g')::INT    AS patrimonio_liquido_lucas,

        REGEXP_REPLACE(bradesco, '[^0-9]', '', 'g')::INT               AS saldo_bradesco_lucas,
        REGEXP_REPLACE(bradesco_investimentos, '[^0-9]', '', 'g')::INT AS saldo_bradesco_investimentos_lucas,
        REGEXP_REPLACE(nubank_investimentos, '[^0-9]', '', 'g')::INT   AS saldo_nubank_investimentos_lucas,
        REGEXP_REPLACE(nubank_cashback, '[^0-9]', '', 'g')::INT        AS saldo_nubank_cashback_lucas,
        REGEXP_REPLACE(bitcoin, '[^0-9]', '', 'g')::INT                AS saldo_bitcoin_lucas,
        REGEXP_REPLACE(daycoval, '[^0-9]', '', 'g')::INT               AS saldo_daycoval_lucas,
        REGEXP_REPLACE(avenue_l, '[^0-9]', '', 'g')::INT               AS saldo_avenue_lucas,
        REGEXP_REPLACE(wise, '[^0-9]', '', 'g')::INT                   AS saldo_wise_lucas,

        REGEXP_REPLACE(patrimonio_jessica_r$, '[^0-9]', '', 'g')::INT  AS patrimonio_liquido_jessica,
        REGEXP_REPLACE(banco_brasil, '[^0-9]', '', 'g')::INT           AS saldo_banco_brasil_jessica,
        REGEXP_REPLACE(sofisa, '[^0-9]', '', 'g')::INT                 AS saldo_sofisa_investimentos_jessica,
        REGEXP_REPLACE(itau, '[^0-9]', '', 'g')::INT                   AS saldo_itau_investimentos_jessica,
        REGEXP_REPLACE(nubank, '[^0-9]', '', 'g')::INT                 AS saldo_nubank_investimentos_jessica,
        REGEXP_REPLACE(avenue_j, '[^0-9]', '', 'g')::INT               AS saldo_avenue_jessica,
        REGEXP_REPLACE(carro, '[^0-9]', '', 'g')::INT                  AS vlr_carro,
        (REPLACE(
            REPLACE(REGEXP_REPLACE(minha_inflacao, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC / 100)::NUMERIC(18, 3)                              AS minha_inflacao,
        (REPLACE(
            REPLACE(REGEXP_REPLACE(ipca, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC / 100)::NUMERIC(18, 3)                              AS ipca,
        (REPLACE(
            REPLACE(REGEXP_REPLACE(igpm, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC / 100)::NUMERIC(18, 3)                              AS igpm,

        (REPLACE(
            REPLACE(REGEXP_REPLACE(selic, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC / 100)::NUMERIC(18, 3)                              AS selic,
        (REPLACE(
            REPLACE(REGEXP_REPLACE(cdi, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC / 100)::NUMERIC(18, 3)                              AS cdi,

        REPLACE("minha_inflacao_acum.", ',', '.')::NUMERIC(18, 3)      AS minha_inflacao_acum,
        REPLACE("ipca_acum.", ',', '.')::NUMERIC(18, 3)                AS ipca_acum,
        REPLACE("igpm_acum.", ',', '.')::NUMERIC(18, 3)                AS igpm_acum,
        REPLACE("selic_acum.", ',', '.')::NUMERIC(18, 3)               AS selic_acum,
        REPLACE("cdi_acum.", ',', '.')::NUMERIC(18, 3)                 AS cdi_acum
    FROM source
)

SELECT * FROM renamed
WHERE mes_base > '2023-08-01'
