{{
  config(
    materialized = 'incremental',
    unique_key = 'game_id',
    tags = ['nhl', 'staging', 'game_id']
  )
}}

with base as (
    select * 
    from {{ ref('stg_base_all_games_details') }}
    {% if is_incremental() %}
    where game_id > (
        select coalesce(max(game_id), 0)
        from {{ this }}
    )
    {% endif %}
),

game_details as (
    select 
        game_id,
        season_id,
        game_type_id,
        game_date,
        game_state,
        -- Campos específicos do game
        (payload -> 'awayTeam' ->> 'id')::int as away_team_id,
        (payload -> 'awayTeam' ->> 'sog')::int as away_team_sog,
        (payload -> 'awayTeam' ->> 'score')::int as away_team_score,
        (payload -> 'awayTeam' ->> 'abbrev') as away_team_abbrev,
        (payload -> 'awayTeam' -> 'placeName' ->> 'default') as away_team_placename,
        (payload -> 'awayTeam' -> 'commonName' ->> 'default') as away_team_commonname,
        case 
            when game_type_id = 1 then 'preseason'
            when game_type_id = 2 then 'regular'
            when game_type_id = 3 then 'playoffs'
        end as game_type_name,
        (payload -> 'homeTeam' ->> 'id')::int as home_team_id,
        (payload -> 'homeTeam' ->> 'sog')::int as home_team_sog,
        (payload -> 'homeTeam' ->> 'score')::int as home_team_score,
        (payload -> 'homeTeam' ->> 'abbrev') as home_team_abbrev,
        (payload -> 'homeTeam' -> 'placeName' ->> 'default') as home_team_placename,
        (payload -> 'homeTeam' -> 'commonName' ->> 'default') as home_team_commonname,
        (payload ->> 'regPeriods') as reg_periods,
        (payload -> 'gameOutcome' ->> 'lastPeriodType') as last_period_type,
        (payload ->> 'startTimeUTC')::timestamp as game_start_timestamp_utc
    from base
)

select * from game_details