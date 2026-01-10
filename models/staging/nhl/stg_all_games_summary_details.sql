{{
  config(
    materialized = 'incremental',
    unique_key = ['game_id'],
    tags = ['nhl', 'staging', 'game_id']
  )
}}

with base as (
    select * 
    from {{ ref('stg_base_all_games_summary_details') }}
    {% if is_incremental() %}
    where game_id >= (select max(game_id) from {{ this }})
    {% endif %}
),

exploded as (
    select
        game_id,
        p ->> 'category' as metric,
        (p ->> 'awayValue') as away_value,
        (p ->> 'homeValue') as home_value
    from base,
    jsonb_array_elements(payload -> 'teamGameStats') as p
),

wide as (
    select
        game_id,
        max(away_value) filter (where metric = 'sog')::int as away_sog,
        max(home_value) filter (where metric = 'sog')::int as home_sog,
        max(away_value) filter (where metric = 'faceoffWinningPctg')::float as away_faceoff_winning_pctg,
        max(home_value) filter (where metric = 'faceoffWinningPctg')::float as home_faceoff_winning_pctg,
        max(away_value) filter (where metric = 'powerPlayPctg')::float as away_powerplay_pctg,
        max(home_value) filter (where metric = 'powerPlayPctg')::float as home_powerplay_pctg,
        max(away_value) filter (where metric = 'pim')::int as away_pim,
        max(home_value) filter (where metric = 'pim')::int as home_pim,
        max(away_value) filter (where metric = 'hits')::int as away_hits,
        max(home_value) filter (where metric = 'hits')::int as home_hits,
        max(away_value) filter (where metric = 'blockedShots')::int as away_blocked_shots,
        max(home_value) filter (where metric = 'blockedShots')::int as home_blocked_shots,
        max(away_value) filter (where metric = 'giveaways')::int as away_giveaways,
        max(home_value) filter (where metric = 'giveaways')::int as home_giveaways,
        max(away_value) filter (where metric = 'takeaways')::int as away_takeaways,
        max(home_value) filter (where metric = 'takeaways')::int as home_takeaways
    from exploded
    group by game_id
)

select * from wide
order by game_id desc