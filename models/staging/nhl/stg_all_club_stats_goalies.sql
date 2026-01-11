{{ 
  config(
    materialized = 'table',
    tags = ['nhl','staging', 'player_id']
  )
}}

with base as (
  select * from {{ ref('stg_base_all_club_stats') }}
),

goalies as (
    select
        base.season_id,
        base.game_type_id,
        'goalie' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'firstName' ->> 'default') as player_first_name,
        (p -> 'lastName' ->> 'default') as player_last_name,
        (p ->> 'wins')::int as wins,
        (p ->> 'goals')::int as goals,
        (p ->> 'saves')::int as saves,
        (p ->> 'losses')::int as losses,
        (p ->> 'points')::int as points,
        (p ->> 'assists')::int as assists,
        (p ->> 'headshot') as player_picture,
        (p ->> 'shutouts')::int as shutouts,
        (p ->> 'timeOnIce')::int as toi,
        (p ->> 'gamesPlayed')::int as games_played,
        (p ->> 'gamesStarted')::int as games_started,
        (p ->> 'goalsAgainst')::int as goals_against,
        (p ->> 'shotsAgainst')::int as shots_against,
        (p ->> 'penaltyMinutes')::int as pim,
        (p ->> 'savePercentage')::float as save_pctg,
        (p ->> 'goalsAgainstAverage')::float as goals_against_average
    from base,
          jsonb_array_elements(payload -> 'goalies') as p
)
select * from goalies