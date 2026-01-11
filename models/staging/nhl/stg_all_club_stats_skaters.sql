{{ 
  config(
    materialized = 'table',
    tags = ['nhl','staging', 'player_id']
  )
}}

with base as (
  select * from {{ ref('stg_base_all_club_stats') }}
),

skaters as (
    select
        season_id,
        game_type_id,
        'skater' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'firstName' ->> 'default') as player_first_name,
        (p -> 'lastName' ->> 'default') as player_last_name,
        (p ->> 'goals')::int as goals,
        (p ->> 'shots')::int as shots,
        (p ->> 'points')::int as points,
        (p ->> 'assists')::int as assists,
        (p ->> 'headshot') as player_picture,
        (p ->> 'plusMinus')::int as plus_minus,
        (p ->> 'gamesPlayed')::int as games_played,
        (p ->> 'positionCode') as position,
        (p ->> 'shootingPctg')::float as shooting_pctg,
        (p ->> 'overtimeGoals')::int as overtime_goals,
        (p ->> 'faceoffWinPctg')::float as faceoff_win_pctg,
        (p ->> 'penaltyMinutes')::int as pim,        
        (p ->> 'powerPlayGoals')::int as powerplay_goals,
        (p ->> 'avgShiftsPerGame')::float as avg_shifts_per_game,
        (p ->> 'gameWinningGoals')::int as game_winning_goals,
        (p ->> 'shorthandedGoals')::int as shorthanded_goals,
        (p ->> 'avgTimeOnIcePerGame')::float as avg_toi_per_game_seconds
    from base,
        jsonb_array_elements(payload -> 'skaters') as p
)
select * from skaters