{{
  config(
    materialized = 'view',
    tags = ['nhl','intermediate'],
    )
}}

with game_summary as (
    select
        game_id,
        season_id,
        game_date,
        game_start_timestamp_et,
        game_number,
        "period" as final_period,
        home_team_id,
        visiting_team_id as away_team_id,
        game_type_id,
        home_score,
        game_state_id,
        visiting_score as away_score,
        case when home_score > visiting_score then home_team_id else visiting_team_id end as winner_team_id
    from {{ ref('stg_all_games_summary') }}
    where has_happened_by_status is true
),
game_details as (
    select 
        game_id,
        away_sog,
        home_sog,
        away_faceoff_winning_pctg,
        home_faceoff_winning_pctg,
        away_powerplay_pctg,
        home_powerplay_pctg,
        away_pim,
        home_pim,
        away_hits,
        home_hits,
        away_blocked_shots,
        home_blocked_shots,
        away_giveaways,
        home_giveaways,
        away_takeaways,
        home_takeaways
    from {{ ref('stg_all_games_summary_details') }} 
),
final as (
    select
        gs.game_id,
        gs.season_id,
        gs.game_date,
        gs.game_start_timestamp_et,
        gs.game_number,
        gs.final_period,
        gs.home_team_id,
        gs.away_team_id,
        gs.game_type_id,
        gs.winner_team_id,
        gs.home_score,
        gs.away_score,

        gd.home_sog,
        gd.away_sog,
        gd.home_faceoff_winning_pctg,
        gd.away_faceoff_winning_pctg,
        gd.home_powerplay_pctg,
        gd.away_powerplay_pctg,
        gd.home_pim,
        gd.away_pim,
        gd.home_hits,
        gd.away_hits,
        gd.home_blocked_shots,
        gd.away_blocked_shots,
        gd.home_giveaways,
        gd.away_giveaways,
        gd.home_takeaways,
        gd.away_takeaways
    from game_summary gs
    left join game_details gd
    on gs.game_id = gd.game_id
    ORDER BY gs.game_id desc
)

select * from final

