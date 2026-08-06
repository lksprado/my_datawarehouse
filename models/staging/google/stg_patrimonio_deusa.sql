{{
  config(
    tags = ['financas', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw','patrimonio_deusa') }}
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

        REGEXP_REPLACE(patrimonio, '[^0-9]', '', 'g')::INT             AS total_patrimonio_liquido,

        REGEXP_REPLACE(bb_saldo, '[^0-9]', '', 'g')::INT               AS saldo_banco_brasil_deusa,
        REGEXP_REPLACE(bb_investimento, '[^0-9]', '', 'g')::INT        AS saldo_banco_brasil_investimentos_deusa,

        REGEXP_REPLACE(bradesco_saldo, '[^0-9]', '', 'g')::INT         AS saldo_bradesco_deusa,
        REGEXP_REPLACE(bradesco_investimentos, '[^0-9]', '', 'g')::INT AS saldo_bradesco_investimentos_deusa,
        REGEXP_REPLACE(nubank_saldo, '[^0-9]', '', 'g')::INT           AS saldo_nubank_deusa,
        REGEXP_REPLACE(nubank_investimentos, '[^0-9]', '', 'g')::INT   AS saldo_nubank_investimentos_deusa,
        REGEXP_REPLACE(nubank_cashback, '[^0-9]', '', 'g')::INT        AS saldo_nubank_cashback_deusa
    FROM source
)

SELECT * FROM renamed
WHERE mes_base > '2023-08-01'
