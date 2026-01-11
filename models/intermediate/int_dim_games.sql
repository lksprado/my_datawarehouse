{{
  config(
    tags = ['nhl','intermediate'],
    )
}}

with 
game_summary as (
  select * from {{ ref('stg_all_games_summary')}}
),
game_details as (
  select * from {{ ref('stg_all_games_details') }}
),
renamed as (
  select 
  gs.game_id,
  gs.game_number,
  gs.season_id,
  gs.game_date,
  gs.game_type_id,
  gs.game_type_name,
  gs.game_start_timestamp_et,
  gs.has_happened_by_status,
  gs.has_happened_by_time,
  gd.game_state,
  gd.regular_periods,
  gd.game_outcome_last_period,
  gd.game_outcome_total_periods,
  gd.special_event_name,
  gd.game_start_timestamp_utc,
  gd.game_schedule_state
  from game_summary gs
  left join game_details gd 
  on gs.game_id = gd.game_id
)
select * from renamed
