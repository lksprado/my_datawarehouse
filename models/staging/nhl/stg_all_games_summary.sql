{{
  config(
    materialized = 'table',
    tags = ['nhl','staging', 'game_id'],
    post_hook = [
        "create index if not exists idx_games_summary on {{ this }} (game_id)"
    ]
    )
}}

with
source as (
    select * from {{ source('raw','nhl_raw_all_games_summary') }}
),

renamed as (
    select
        (payload ->> 'id')::int as game_id,
        (payload ->> 'season')::int as season_id,
        (payload ->> 'gameDate')::date as game_date,
        (payload ->> 'gameType')::int as game_type_id,
        (payload ->> 'homeScore')::int as home_score,
        (payload ->> 'homeTeamId')::int as home_team_id,
        (payload ->> 'gameStateId')::int as game_state_id,
        (payload ->> 'visitingScore')::int as visiting_score,
        (payload ->> 'visitingTeamId')::int as visiting_team_id,
        (payload ->> 'easternStartTime')::timestamp as game_start_timestamp_et,
        (payload ->> 'gameScheduleStateId')::int as game_schedule_state_id,
        (payload ->> 'gameNumber') as game_number,
        (payload ->> 'period') as period,
        case
            when payload ->> 'gameType' like '1' then 'preseason'
            when payload ->> 'gameType' like '2' then 'regular'
            when payload ->> 'gameType' like '3' then 'playoffs'
        end as game_type_name,
        (payload ->> 'gameDate')::date < current_date as has_happened_by_time,
        (payload ->> 'gameStateId')::int = 7 as has_happened_by_status
    from source
    where (payload ->> 'gameScheduleStateId')::int = 1
)

select * from renamed
