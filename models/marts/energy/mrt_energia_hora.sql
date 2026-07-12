{{
  config(
    tags = ['energia', 'marts'],
  )
}}

WITH
tab_energia_hora AS (
    SELECT *
    FROM {{ ref('stg_solar_hourly_energy') }}
)

SELECT * FROM tab_energia_hora
