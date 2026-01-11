{{
  config(
    materialized = 'view',
    tags = ['nhl','intermediate'],
    )
}}

-- Unpivot home stats
with 
wide_game_stats as (
    select * from {{ref('int_fct_games_team_stats_wide')}}
),
home_stats as (
    select
        g.game_id,
        g.season_id,
        g.game_type_id,
        g.game_date,
        g.home_team_id as team_id,
        'home' as team_side,
        v.metric_name,
        v.metric_value
    from wide_game_stats g
    cross join lateral (
        values
            ('score', g.home_score),
            ('shots_on_goal', g.home_sog),
            ('faceoff_win_pctg', g.home_faceoff_winning_pctg),
            ('powerplay_pctg', g.home_powerplay_pctg),
            ('penalty_minutes', g.home_pim),
            ('hits', g.home_hits),
            ('blocked_shots', g.home_blocked_shots),
            ('giveaways', g.home_giveaways),
            ('takeaways', g.home_takeaways)
    ) as v(metric_name, metric_value)
),
away_stats as (
    select
        g.game_id,
        g.season_id,
        g.game_type_id,
        g.game_date,
        g.away_team_id as team_id,
        'away' as team_side,
        v.metric_name,
        v.metric_value
    from wide_game_stats g
    cross join lateral (
        values
            ('score', g.away_score),
            ('shots_on_goal', g.away_sog),
            ('faceoff_win_pctg', g.away_faceoff_winning_pctg),
            ('powerplay_pctg', g.away_powerplay_pctg),
            ('penalty_minutes', g.away_pim),
            ('hits', g.away_hits),
            ('blocked_shots', g.away_blocked_shots),
            ('giveaways', g.away_giveaways),
            ('takeaways', g.away_takeaways)
    ) as v(metric_name, metric_value)
),
unioned as (
    select * from home_stats
    union all
    select * from away_stats
)
select * from unioned

