{{
  config(
    materialized = 'incremental',
    unique_key = ['game_id', 'event_id'],
    incremental_strategy = 'delete+insert',
    tags = ['nhl','staging', 'game_id']
    )
}}

with 
source as (
    select * from {{ source('raw', 'nhl_raw_all_play_by_play') }}
    {% if is_incremental() %}
    -- Filtra na SOURCE antes de processar
    where (payload ->> 'gameDate')::date > (select max(game_date) from {{ this }})
    {% endif %}
),
renamed as (
    select
        (payload ->> 'id')::int as game_id,
        (payload ->> 'season')::int as season_id,
        (payload -> 'clock' ->> 'running')::boolean as is_running,
        (payload -> 'clock' ->> 'timeRemaining') as time_remaining,
        (payload -> 'clock' ->> 'inIntermission')::boolean as is_in_intermission,
        (payload -> 'clock' ->> 'secondsRemaining') as seconds_remaining,
        (payload ->> 'otInUse')::boolean as overtime_in_use,
        (payload ->> 'gameDate')::date as game_date,
        (payload ->> 'gameType')::int as game_type_id,
        (payload ->> 'gameState') as game_state,
        (payload ->> 'maxPeriods')::int as max_periods,
        (payload ->> 'regPeriods')::int as reg_periods,
        (payload -> 'gameOutcome' ->> 'lastPeriodType') as outcome_last_period_type,
        (payload ->> 'startTimeUTC')::timestamp as start_time_utc,
        (payload ->> 'shootoutInUse')::boolean as shootout_in_use,
        (payload ->> 'gameScheduleState') as game_schedule_state,
        (p ->> 'eventId')::int as event_id,
        (p ->> 'typeCode')::int as type_code,
        (p ->> 'sortOrder')::int as sort_order,
        (p ->> 'typeDescKey') as type_desc_key,
        (p ->> 'timeInPeriod') as time_in_period,
        (p ->> 'situationCode')::int as situation_code,
        (p -> 'periodDescriptor' ->> 'number')::int as period_number,
        (p -> 'periodDescriptor' ->> 'periodType') as period_type,
        (p ->> 'homeTeamDefendingSide') as home_team_defending_side,
        (p -> 'details' ->> 'xCoord')::int as x_coord,
        (p -> 'details' ->> 'yCoord')::int as y_coord,
        (p -> 'details' ->> 'descKey') as desc_key,
        (p -> 'details' ->> 'duration')::int as duration,
        (p -> 'details' ->> 'typeCode') as penalty_type_code,
        (p -> 'details' ->> 'zoneCode') as zone_code,
        (p -> 'details' ->> 'drawnByPlayerId')::int as drawn_by_player_id,
        (p -> 'details' ->> 'eventOwnerTeamId')::int as event_owner_team_id,
        (p -> 'details' ->> 'committedByPlayerId')::int as commited_by_player_id,
        (p -> 'details' ->> 'hitteePlayerId')::int as hittee_player_id,
        (p -> 'details' ->> 'hittingPlayerId')::int as hitting_player_id,
        (p -> 'details' ->> 'losingPlayerId')::int as losing_player_id,
        (p -> 'details' ->> 'winningPlayerId')::int as winning_player_id,
        (p -> 'details' ->> 'reason') as reason,
        (p -> 'details' ->> 'secondaryReason') as secondary_reason,
        (p -> 'details' ->> 'awaySOG')::int as away_sog,
        (p -> 'details' ->> 'homeSOG')::int as home_sog,
        (p -> 'details' ->> 'shotType') as shot_type,
        (p -> 'details' ->> 'goalieInNetId')::int as goalie_in_net_player_id,
        (p -> 'details' ->> 'shootingPlayerId')::int as shooting_player_id,
        (p -> 'details' ->> 'awayScore')::int as away_score,
        (p -> 'details' ->> 'homeScore')::int as home_score
    from source,
          jsonb_array_elements(payload -> 'plays') as p
)
select * from renamed