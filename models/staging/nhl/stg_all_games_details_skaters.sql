{{ 
  config(
    materialized = 'incremental',
    unique_key = ['game_id', 'player_id'],
    tags = ['nhl', 'staging', 'player_id']
  )
}}

with base as (
    select * 
    from {{ ref('stg_base_all_games_details') }}
    {% if is_incremental() %}
    where game_id not in (
        select distinct game_id 
        from {{ this }}
    )
    {% endif %}
),

away_defense as (
    select
        game_id,
        -- Campos específicos do game
        'away' as team_side,
        'defense' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'goals')::int as goals,
        (p ->> 'assists')::int as assists,
        (p ->> 'points')::int as points,
        (p ->> 'sog')::int as shots_on_goal,
        (p ->> 'hits')::int as hits,
        (p ->> 'blockedShots')::int as blocked_shots,
        (p ->> 'plusMinus')::int as plus_minus
    from base,
    jsonb_array_elements(payload -> 'playerByGameStats' -> 'awayTeam' -> 'defense') as p
),

away_forwards as (
    select
        game_id,
        -- Campos específicos
        'away' as team_side,
        'forward' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'goals')::int as goals,
        (p ->> 'assists')::int as assists,
        (p ->> 'points')::int as points,
        (p ->> 'sog')::int as shots_on_goal,
        (p ->> 'hits')::int as hits,
        (p ->> 'blockedShots')::int as blocked_shots,
        (p ->> 'plusMinus')::int as plus_minus
    from base,
    jsonb_array_elements(payload -> 'playerByGameStats' -> 'awayTeam' -> 'forwards') as p
),

home_defense as (
    select
        game_id,
        -- Campos específicos
        'home' as team_side,
        'defense' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'goals')::int as goals,
        (p ->> 'assists')::int as assists,
        (p ->> 'points')::int as points,
        (p ->> 'sog')::int as shots_on_goal,
        (p ->> 'hits')::int as hits,
        (p ->> 'blockedShots')::int as blocked_shots,
        (p ->> 'plusMinus')::int as plus_minus
    from base,
    jsonb_array_elements(payload -> 'playerByGameStats' -> 'homeTeam' -> 'defense') as p
),

home_forwards as (
    select
        game_id,
        -- Campos específicos
        'home' as team_side,
        'forward' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'goals')::int as goals,
        (p ->> 'assists')::int as assists,
        (p ->> 'points')::int as points,
        (p ->> 'sog')::int as shots_on_goal,
        (p ->> 'hits')::int as hits,
        (p ->> 'blockedShots')::int as blocked_shots,
        (p ->> 'plusMinus')::int as plus_minus
    from base,
    jsonb_array_elements(payload -> 'playerByGameStats' -> 'homeTeam' -> 'forwards') as p
),

all_skaters as (
    select * from away_defense
    union all
    select * from away_forwards
    union all
    select * from home_defense
    union all
    select * from home_forwards
)

select * from all_skaters