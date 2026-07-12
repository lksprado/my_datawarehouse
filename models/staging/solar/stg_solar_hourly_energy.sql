WITH
source AS (SELECT * FROM {{ source('raw', 'solar_hourly_energy') }}),

renamed AS (
    SELECT
        datetime::TIMESTAMP AS dt_hora,
        energy::FLOAT       AS kwh
    FROM source
)

SELECT * FROM renamed
