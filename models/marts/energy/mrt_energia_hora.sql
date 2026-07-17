{{
  config(
    tags = ['energia', 'marts'],
  )
}}

WITH
tab_energia_hora AS (
    SELECT
        t1.dt,
        t1.dt_hora,
        t2.day_of_month     AS dia_mes,
        t2.day_of_year      AS dia_ano,
        t2.week_of_year     AS semana,
        t2.year_number      AS ano,
        t2.month_name       AS nome_mes,
        t2.month_name_short AS nome_mes_abreviado,
        t1.kwh
    FROM {{ ref('stg_solar_hourly_energy') }}
)

SELECT * FROM tab_energia_hora
