{{ 
  config(
    materialized = 'incremental',
    unique_key = ['player_id', 'game_id', 'game_type_id'],
    tags = ['nhl','staging', 'player_id'],
    post_hook = [
        "create index if not exists idx_games_log on {{ this }} (game_id, player_id)",
        "create index if not exists idx_player_log_date on {{ this }} (player_id, game_date)"
    ]
  )
}}

with source as (

    select *
    from {{ source('raw', 'nhl_raw_all_player_game_log') }}
),

stats_games as (
    select
        split_part(source_filename, '_', 1)::int as player_id,
        (payload ->> 'seasonId')::int as season_id,
        (payload ->> 'gameTypeId')::int as game_type_id,
        (p ->> 'pim')::int as pim,
        (p ->> 'goals')::int as goals,
        (p ->> 'shots')::int as shots,
        (p ->> 'gameId')::int as game_id,
        (p ->> 'points')::int as points,
        (p ->> 'shifts')::int as shifts,
        (p ->> 'assists')::int as assists,
        (p ->> 'otGoals')::int as overtime_goals,
        (p ->> 'gameDate')::date as game_date,
        (p ->> 'plusMinus')::int as plus_minus,
        (p ->> 'powerPlayGoals')::int as powerplay_goals,
        (p ->> 'powerPlayPoints')::int as powerplay_points,
        (p ->> 'gameWinningGoals')::int as game_winning_goals,
        (p ->> 'shorthandedGoals')::int as shorthanded_goals,
        (p ->> 'shorthandedPoints')::int as shorthanded_points,
        (p ->> 'savePctg')::float as save_pctg,
        (p ->> 'shutouts')::int as is_shutout,
        (p ->> 'gamesStarted')::int as is_starter,
        (p ->> 'goalsAgainst')::int as goals_against,
        (p ->> 'shotsAgainst')::int as shots_against,
        (p ->> 'toi') as toi,
        (p ->> 'teamAbbrev') as team_abbrev,
        (p ->> 'homeRoadFlag') as home_road_flag,
        (p ->> 'opponentAbbrev') as opponent_abbrev,
        (p ->> 'decision') as game_decision
    from source,
        jsonb_array_elements(payload -> 'gameLog') as p
)

select
    *,
    row_number() over (partition by player_id, season_id order by game_id)::int
        as game_played_number
from stats_games
{% if is_incremental() %}
    where game_date >= (select max(game_date) from {{ this }})
{% endif %}