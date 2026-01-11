{{
  config(
    materialized = 'view',
    tags = ['nhl','intermediate'],
    )
}}

with 
pbp as (
    select
    game_id,
    event_id,
    row_number() over (partition by game_id order by sort_order) as event_order,
    type_desc_key as event_description,
    time_in_period,
    period_number,
    period_type,
    home_team_defending_side,
    x_coord,
    y_coord,
    desc_key as penalty_description,
    duration as penalty_duration,
    penalty_type_code,
    case 
        when zone_code = 'O' then 'Offensive' 
        when zone_code = 'N' then 'Neutral'
        when zone_code = 'D' then 'Defensive'
    end as event_zone,
    drawn_by_player_id as penalty_drawn_by_player_id,
    commited_by_player_id as penalty_committed_by_player_id,
    event_owner_team_id,
    hittee_player_id,
    hitting_player_id,
    losing_player_id,
    winning_player_id,
    reason as event_reason,
    secondary_reason as event_secondary_reason,
    away_sog,
    home_sog,
    shot_type,
    goalie_in_net_player_id,
    shooting_player_id,
    away_score,
    home_score
    from {{ ref('stg_all_play_by_play') }}
),
final as (
    select
    *
    from pbp
)

select * from final

