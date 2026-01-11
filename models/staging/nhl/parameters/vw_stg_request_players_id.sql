{{ 
  config(
    materialized = 'view',
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
goalies_club_stats as (
  select 
  distinct
  player_id
  from {{ ref('stg_all_club_stats_goalies') }} t1
  inner join base t2
  on t1.season_id = t2.season_id
    and t1.game_type_id = t2.game_type_id
),
skaters_club_stats as (
  select 
  distinct
  player_id
  from {{ ref('stg_all_club_stats_skaters') }} t1
  inner join base t2
  on t1.season_id = t2.season_id
    and t1.game_type_id = t2.game_type_id
),
final as (
  select * from goalies_club_stats
  union
  select * from skaters_club_stats
)
select 
* 
from final