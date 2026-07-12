{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

with
source as (
    select 
    TO_DATE(
        LPAD(data::text, 8, '0'),
        'DDMMYYYY'
    ) AS data_usd,
    venda::numeric / POWER(10, LENGTH(venda::text) - 1) AS vlr_usd
    from {{ ref('cotacao_usd')}}
),
ultimo_dia_disponivel AS (
    SELECT DISTINCT ON (DATE_TRUNC('month', data_usd))
        data_usd,
        vlr_usd::numeric(18,2) as vlr_usd
    FROM source
    ORDER BY DATE_TRUNC('month', data_usd), data_usd DESC
),
final as (
  SELECT
      (
          DATE_TRUNC('month', data_usd)
          + INTERVAL '1 month'
          - INTERVAL '1 day'
      )::date AS data_referencia,
      vlr_usd
  FROM ultimo_dia_disponivel
  ORDER BY data_referencia
  )
select * from final