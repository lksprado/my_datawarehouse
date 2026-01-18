{{
  config(
    materialized = 'ephemeral',
    tags = ['nhl','intermediate'],
    )
}}


with 
teams as (
    select * from {{ref('int_dim_teams') }}
),
players as (
    select
        player_id,
        player_name
    from {{ ref('int_dim_players') }}
),
game_log as (
    select 
    season_id,
    player_id,
    game_id,
    t2.team_id
    from {{ ref('stg_all_player_game_log') }} t1
    left join teams t2 
    on t1.team_abbrev = t2.team_code and t1.season_id >= t2.first_season_id
    group by 1, 2, 3, 4
),
final as (
    select
    t1.player_id,
    t1.player_name,
    t2.game_id,
    t2.team_id
    from players t1 
    inner join game_log t2 
    on t1.player_id =  t2.player_id
)
select * from final
