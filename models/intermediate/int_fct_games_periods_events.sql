{{
  config(
    materialized = 'view',
    tags = ['nhl','intermediate']
    )
}}

with
game_periods as (
  select * from {{ ref('stg_all_games_summary_periods') }}
),
game_events as (
  select * from {{ ref('int_fct_games_events') }}
),
games as (
  select  * from {{ ref('int_dim_games') }}
),
games_norm as (
  select
      game_id,
      home_team_id as team_id,
      'home' as home_or_away_flag
  from games
  union all
  select
      game_id,
      away_team_id as team_id,
      'away' as home_or_away_flag
  from games
),
game_events_agg as (
  select 
  ge.game_id,
  ge.period_number,
  sum(case
    when ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'home'
    then penalty_duration end ) as home_penalties,
  sum(case
    when ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'away'
    then penalty_duration end ) as away_penalties,

  count(case
    when event_description = 'hit'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'home'
    then event_id end ) as home_hits,
  count(case
    when event_description = 'hit'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'away'
    then event_id end ) as away_hits,

  count(case
    when event_description = 'giveway'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'home'
    then event_id end ) as home_giveaways,
  count(case
    when event_description = 'giveway'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'away'
    then event_id end ) as away_giveaways,

  count(case
    when event_description = 'takeway'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'home'
    then event_id end ) as home_takeaways,
  count(case
    when event_description = 'takeway'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'away'
    then event_id end ) as away_takeaways,

  count(case
    when event_description = 'blocked-shot'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'home'
    then event_id end ) as home_blocked_shots,
  count(case
    when event_description = 'blocked-shot'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'away'
    then event_id end ) as away_blocked_shots,

  count(case
    when event_description = 'missed-shot'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'home'
    then event_id end ) as home_missed_shots,
  count(case
    when event_description = 'missed-shot'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'away'
    then event_id end ) as away_missed_shots,

  count(case
    when event_description = 'faceoff'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'home'
    then event_id end ) as home_faceoff_won,
  count(case
    when event_description = 'faceoff'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'away'
    then event_id end ) as away_faceoff_won,

  count(case
    when event_description = 'failed-shot-attempt'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'home'
    then event_id end ) as home_failed_shot_attempt,
  count(case
    when event_description = 'failed-shot-attempt'
      and ge.event_owner_team_id = gn.team_id
      and gn.home_or_away_flag = 'away'
    then event_id end ) as away_failed_shot_attempt
  from game_events ge 
  left join games_norm gn
  on ge.game_id = gn.game_id
  group by 
  ge.game_id,
  ge.period_number
),
final as (
  select
  gp.game_id,
  gp.period_number,
  gp.home_goals,
  gp.away_goals,
  gp.home_shots,
  gp.away_shots,
  gea.home_penalties,
  gea.away_penalties,
  gea.home_hits,
  gea.away_hits,
  gea.home_giveaways,
  gea.away_giveaways,
  gea.home_takeaways,
  gea.away_takeaways,
  gea.home_blocked_shots,
  gea.away_blocked_shots,
  gea.home_missed_shots,
  gea.away_missed_shots,
  gea.home_faceoff_won,
  gea.away_faceoff_won,
  gea.home_failed_shot_attempt,
  gea.away_failed_shot_attempt
  from game_periods gp
  left join game_events_agg gea
  on gp.game_id = gea.game_id and gp.period_number = gea.period_number
)
select * from final