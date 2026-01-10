{{
  config(
    materialized = 'incremental',
    unique_key = ['game_id', 'period_number'],
    tags = ['nhl', 'staging']
  )
}}

with base as (
    select * 
    from {{ ref('stg_base_all_games_summary_details') }}
    {% if is_incremental() %}
    where game_id >= (select max(game_id) from {{ this }})
    {% endif %}
),

shots as (
    select
        game_id,
        (p -> 'periodDescriptor' ->> 'number')::int as period_number,
        (p -> 'periodDescriptor' ->> 'periodType') as period_type,
        (p ->> 'away')::int as away_shots,
        (p ->> 'home')::int as home_shots
    from base,
    jsonb_array_elements(payload -> 'shotsByPeriod') as p
),

goals as (
    select
        game_id,
        (p -> 'periodDescriptor' ->> 'number')::int as period_number,
        (p -> 'periodDescriptor' ->> 'periodType') as period_type,
        (p ->> 'away')::int as away_goals,
        (p ->> 'home')::int as home_goals
    from base,
    jsonb_array_elements(payload -> 'linescore' -> 'byPeriod') as p
),

joined as (
    select
        s.game_id,
        s.period_number,
        s.period_type,
        s.away_shots,
        s.home_shots,
        g.away_goals,
        g.home_goals
    from shots s 
    join goals g
        on s.game_id = g.game_id 
        and s.period_number = g.period_number
)

select * from joined
order by game_id, period_number