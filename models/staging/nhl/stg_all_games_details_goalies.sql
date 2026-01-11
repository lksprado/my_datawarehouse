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

away_goalies as (
    select
        game_id,
        -- Campos específicos
        'away' as team_side,
        'goalie' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'starter')::boolean as is_starter,
        (p ->> 'goalsAgainst')::int as goals_against,
        (p ->> 'shotsAgainst')::int as shots_against,
        split_part((p ->> 'saveShotsAgainst'), '/', 1)::int as saves,
        (p ->> 'powerPlayGoalsAgainst')::int as powerplay_goals_against,
        split_part((p ->> 'powerPlayShotsAgainst'), '/', 1)::int as powerplay_saves,
        split_part((p ->> 'powerPlayShotsAgainst'), '/', 2)::int as powerplay_shots_against,
        (p ->> 'shorthandedGoalsAgainst')::int as shorthanded_goals_against,
        (p ->> 'evenStrengthGoalsAgainst')::int as evenstrenght_goals_against,
        split_part((p ->> 'evenStrengthShotsAgainst'), '/', 1)::int as evenstrenght_saves,
        split_part((p ->> 'evenStrengthShotsAgainst'), '/', 2)::int as evenstrenght_shots_against
    from base,
    jsonb_array_elements(payload -> 'playerByGameStats' -> 'awayTeam' -> 'goalies') as p
),

home_goalies as (
    select
        game_id,
        -- Campos específicos
        'home' as team_side,
        'goalie' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'starter')::boolean as is_starter,
        (p ->> 'goalsAgainst')::int as goals_against,
        (p ->> 'shotsAgainst')::int as shots_against,
        split_part((p ->> 'saveShotsAgainst'), '/', 1)::int as saves,
        (p ->> 'powerPlayGoalsAgainst')::int as powerplay_goals_against,
        split_part((p ->> 'powerPlayShotsAgainst'), '/', 1)::int as powerplay_saves,
        split_part((p ->> 'powerPlayShotsAgainst'), '/', 2)::int as powerplay_shots_against,
        (p ->> 'shorthandedGoalsAgainst')::int as shorthanded_goals_against,
        (p ->> 'evenStrengthGoalsAgainst')::int as evenstrenght_goals_against,
        split_part((p ->> 'evenStrengthShotsAgainst'), '/', 1)::int as evenstrenght_saves,
        split_part((p ->> 'evenStrengthShotsAgainst'), '/', 2)::int as evenstrenght_shots_against
    from base,
    jsonb_array_elements(payload -> 'playerByGameStats' -> 'homeTeam' -> 'goalies') as p
)

select * from away_goalies
union all
select * from home_goalies