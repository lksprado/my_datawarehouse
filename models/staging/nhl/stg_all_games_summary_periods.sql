{{
  config(
    materialized = 'incremental',
    unique_key = ['game_id', 'period_number'],
    tags = ['nhl', 'staging'],
    post_hook = [
        "create index if not exists idx_games_summary_periods on {{ this }} (game_id, period_number)"
    ]
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
        (p ->> 'away')::int as away_shots,
        (p ->> 'home')::int as home_shots,
        (p -> 'periodDescriptor' ->> 'periodType') as period_type
    from base,
        jsonb_array_elements(payload -> 'shotsByPeriod') as p
),

goals as (
    select
        game_id,
        (p -> 'periodDescriptor' ->> 'number')::int as period_number,
        (p ->> 'away')::int as away_goals,
        (p ->> 'home')::int as home_goals,
        (p -> 'periodDescriptor' ->> 'periodType') as period_type
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
    from shots as s
    inner join goals as g
        on
            s.game_id = g.game_id
            and s.period_number = g.period_number
)

select * from joined
order by game_id, period_number