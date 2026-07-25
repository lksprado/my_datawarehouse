{{ config(
    tags=["datas"]
) }}

WITH
date_dimension AS (
    SELECT * FROM {{ ref("int_dates") }}
),

datas_especiais AS (
    SELECT * FROM {{ ref('stg_datas_especiais') }}
),

final AS (
    SELECT
        d.*,
        CAST(TO_CHAR(d.date_day, 'YYYYMMDD') AS INTEGER)   AS data_sk,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM datas_especiais me
                WHERE me.mes_num = d.month_of_year
                    AND d.year_number >= COALESCE(me.ano_inicio, d.year_number)
            ) THEN 1 ELSE 0
        END                                                AS fl_mes_especial,
        CASE WHEN de.motivo IS NOT NULL THEN 1 ELSE 0 END  AS fl_data_especial,
        COALESCE(de.motivo, 'NORMAL')                      AS motivo
    FROM date_dimension d
    LEFT JOIN datas_especiais de
        ON d.month_of_year = de.mes_num
        AND d.day_of_month = de.dia
        -- eventos com data de início (ex.: casamento) só valem a partir do ano informado
        AND d.year_number >= COALESCE(de.ano_inicio, d.year_number)
)

SELECT * FROM final ORDER BY data_sk 
