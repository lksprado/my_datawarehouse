{{ 
  config(
    materialized = 'view',
    unique_key = ['player_id'],
    tags = ['nhl','staging', 'player_id']
  )
}}

with
base as (
  select
    season_id,
    max(game_type_id) as game_type_id
  from {{ ref('stg_all_games_summary') }}
  where season_id = (select max(season_id) from {{ ref('stg_all_games_summary') }})
  group by season_id
),
players as (
  select * from {{ ref('vw_stg_request_players_id')}}
),
final as (
  select * from base,
    players
)
select * from final