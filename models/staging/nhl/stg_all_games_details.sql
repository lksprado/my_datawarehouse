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
        regular_periods,
        game_outcome_last_period,
        game_outcome_total_periods,
        special_event_name,
        game_start_timestamp_utc,
        game_schedule_state,
        -- Campos específicos do game
        (payload -> 'awayTeam' ->> 'id')::int as away_team_id,
        (payload -> 'awayTeam' ->> 'sog')::int as away_team_sog,
        (payload -> 'awayTeam' ->> 'score')::int as away_team_score,
        (payload -> 'awayTeam' ->> 'abbrev') as away_team_abbrev,
        (payload -> 'awayTeam' -> 'placeName' ->> 'default') as away_team_placename,
        (payload -> 'awayTeam' -> 'commonName' ->> 'default') as away_team_commonname,
        (payload -> 'awayTeam' ->> 'logo') as away_team_logo,
        (payload -> 'awayTeam' ->> 'darkLogo') as away_team_darklogo,
        (payload -> 'homeTeam' ->> 'id')::int as home_team_id,
        (payload -> 'homeTeam' ->> 'sog')::int as home_team_sog,
        (payload -> 'homeTeam' ->> 'score')::int as home_team_score,
        (payload -> 'homeTeam' ->> 'abbrev') as home_team_abbrev,
        (payload -> 'homeTeam' -> 'placeName' ->> 'default') as home_team_placename,
        (payload -> 'homeTeam' -> 'commonName' ->> 'default') as home_team_commonname,
        (payload -> 'homeTeam' ->> 'logo') as home_team_logo,
        (payload -> 'homeTeam' ->> 'darkLogo') as home_team_darklogo
    from base
)

select * from game_details