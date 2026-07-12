{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

WITH
source_legacy AS (
    SELECT * FROM {{ ref('stg_avenue_legacy') }}
),

source_current AS (
    SELECT * FROM {{ ref('stg_avenue_current') }}
),

unioned AS (
    SELECT * FROM source_current
    UNION ALL
    SELECT * FROM source_legacy
)

SELECT * FROM unioned
ORDER BY period_start
