{{
  config(
    tags = ['energia', 'marts'],
  )
}}

WITH
tab_energia AS (
    SELECT *
    FROM {{ ref('stg_solar_daily_energy') }}
),

tab_clima AS (
    SELECT *
    FROM {{ ref('stg_weather_daily') }}
),

final AS (
    SELECT
        t1.dt,
        t3.day_of_month     AS dia_mes,
        t3.day_of_year      AS dia_ano,
        t3.week_of_year     AS semana,
        t3.year_number      AS ano,
        t3.month_name       AS nome_mes,
        t3.month_name_short AS nome_mes_abreviado,
        t1.duracao_geracao_horas,
        t1.total_kwh,
        t1.max_kwh,
        t2.nebulosidade_tarde,
        t2.humidade_tarde,
        t2.precipitacao_total,
        t2.temperatura_min,
        t2.temperatura_max,
        t2.temperatura_manha,
        t2.temperatura_tarde,
        t2.temperatura_noite,
        t2.pressao_tarde,
        t2.velocidade_vento_max,
        t2.direcao_vento_max
    FROM tab_energia AS t1
    INNER JOIN tab_clima AS t2
        ON t1.dt = t2.dt
    INNER JOIN {{ref('dim_datas')}} t3
        ON t1.dt = t3.date_day
    WHERE t1.dt > DATE '2021-09-16'  -- data de instalação do sistema solar
)

SELECT * FROM final
