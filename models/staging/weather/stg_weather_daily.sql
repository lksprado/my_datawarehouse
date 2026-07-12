WITH
source AS (SELECT * FROM {{ source('raw', 'openweather_daily') }}),

renamed AS (
    SELECT
        date::DATE                   AS dt,
        cloud_cover_afternoon::FLOAT AS nebulosidade_tarde,
        humidity_afternoon::FLOAT    AS humidade_tarde,
        precipitation_total::FLOAT   AS precipitacao_total,
        temperature_min::FLOAT       AS temperatura_min,
        temperature_max::FLOAT       AS temperatura_max,
        temperature_afternoon::FLOAT AS temperatura_tarde,
        temperature_night::FLOAT     AS temperatura_noite,
        temperature_morning::FLOAT   AS temperatura_manha,
        pressure_afternoon::FLOAT    AS pressao_tarde,
        wind_max_speed::FLOAT        AS velocidade_vento_max,
        wind_max_direction::FLOAT    AS direcao_vento_max
    FROM source
)

SELECT * FROM renamed
