{{
  config(
    materialized = 'view',
    tags = ['nhl','intermediate'],
    )
}}

with 
players_stats as (
    select
    game_id,
    case when home_road_flag like 'H' then 'home' else 'away' end as team_side,
    sum(points) as points,
    sum(assists) as assists,
    sum(powerplay_goals) as powerplay_goals,
    sum(powerplay_points) as powerplay_points,
    sum(shorthanded_goals) as shorthanded_goals,
    sum(shots_against) as shots_against
    from {{ ref('stg_all_player_game_log') }}
    group by game_id, home_road_flag
),
final as (
    select
    game_id,
    team_side,
    points,
    assists,
    powerplay_goals,
    powerplay_points,
    shorthanded_goals,
    shots_against
from players_stats
)

select * from final

