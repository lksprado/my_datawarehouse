{{ 
  config(
    materialized = 'ephemeral',
    tags = ['nhl', 'staging']
  )
}}

select 
    payload,
    (payload ->> 'season')::int as season_id,
    (payload ->> 'gameType')::int as game_type_id
from {{ source('raw', 'nhl_raw_all_club_stats') }}