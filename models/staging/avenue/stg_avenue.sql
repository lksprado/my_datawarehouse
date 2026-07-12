{{
  config(
    materialized = 'table',
    tags = ['investimentos', 'staging'],
  )
}}

with
source_legacy as (
    select * from {{ ref('stg_avenue_legacy')}}
),
source_current as (
    select * from {{ ref('stg_avenue_current')}}
),
unioned as (
    select * from source_current
    union all
    select * from source_legacy
)
select * from unioned order by period_start