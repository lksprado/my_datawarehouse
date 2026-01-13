{{ 
  config(
    materialized = 'view',
    tags = ['nhl','staging', 'parameters']
  )
}}

with source as (

    select payload::int as season_id
    from {{ source('raw','nhl_raw_all_seasons_id') }}

),

max_season as (

    select max(season_id) as max_season_id
    from source

),

final as (

    select distinct
        s.season_id,
        s.season_id = m.max_season_id as is_current
    from source as s
    cross join max_season as m
    order by s.season_id desc

)

select * from final
