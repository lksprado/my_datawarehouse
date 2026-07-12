WITH
source AS (SELECT * FROM {{ source('raw', 'solar_daily_energy') }}),

renamed AS (
    SELECT
        date::DATE    AS dt,
        duration::INT AS duracao_geracao_horas,
        total::FLOAT  AS total_kwh,
        co2::FLOAT,
        max::FLOAT    AS max_kwh
    FROM source
)

SELECT * FROM renamed
