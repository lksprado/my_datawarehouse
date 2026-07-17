{{
  config(
    tags = ['investimentos', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw','luz') }}
),

renamed AS (
    SELECT
        REPLACE(
            REPLACE(REGEXP_REPLACE(fatura, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2) AS vlr_fatura,
        kwh,
        dias,
        REPLACE(
            REPLACE(REGEXP_REPLACE(kwh_dia, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2) AS kwh_dia,
        REPLACE(
            REPLACE(REGEXP_REPLACE(preco_kwh, '[^0-9,.]', '', 'g'), '.', ''),
            ',',
            '.'
        )::NUMERIC(18, 2) AS preco_kwh,
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
        )                 AS mes
    FROM source
)

SELECT * FROM renamed
