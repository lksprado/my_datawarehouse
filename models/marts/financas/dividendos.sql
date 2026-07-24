{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}


WITH
datas AS (
    SELECT DISTINCT
        month_start_date,
        quarter_of_year,
        year_number
    FROM {{ ref('dim_datas') }}
),

pessoas AS (
    SELECT DISTINCT pessoa
    FROM {{ ref('int_renda_passiva') }}
),

dividendos AS (
    SELECT
        mes_base,
        pessoa,
        SUM(vlr_liquido_brl) AS vlr_liquido_brl
    FROM {{ ref('int_renda_passiva') }}
    GROUP BY 1, 2
),

periodo AS (
    SELECT
        MIN(mes_base) AS data_min,
        MAX(mes_base) AS data_max
    FROM dividendos
),

spine AS (
    SELECT
        datas.month_start_date,
        datas.quarter_of_year,
        datas.year_number,
        pessoas.pessoa
    FROM datas
    CROSS JOIN pessoas
    CROSS JOIN periodo
    WHERE datas.month_start_date BETWEEN periodo.data_min AND periodo.data_max
),

final AS (
    SELECT
        spine.month_start_date              AS mes_base,
        spine.year_number                   AS ano,
        spine.quarter_of_year               AS trimestre,
        spine.pessoa,
        COALESCE(dividendos.vlr_liquido_brl, 0) AS vlr_liquido_brl
    FROM spine
    LEFT JOIN dividendos
        ON spine.month_start_date = dividendos.mes_base
        AND spine.pessoa = dividendos.pessoa
    ORDER BY spine.month_start_date
)

SELECT * FROM final
