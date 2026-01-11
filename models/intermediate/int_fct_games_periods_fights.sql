{{
  config(
    materialized = 'view',
    unique_key=['game_id', 'period_number'],
    tags = ['nhl','intermediate'],
    )
}}

with
game_events as (
  select * from {{ ref('int_fct_games_events') }} WHERE penalty_description = 'fighting'
),
final as (
  select
  game_id,
  period_number,
  count(*)/2 as fights
  from game_events
  group by 
  game_id,
  period_number
)
select * from final