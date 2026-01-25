{{
  config(
    tags = ['nhl','intermediate'],
    )
}}


with source as (
    select
        *,
        row_number() over (
            partition by player_id
        ) as rn
    from {{ ref('stg_all_players') }}
    where season_type = 'regular'
),

players as (
    select
        player_id,
        player_name
    from {{ ref ('stg_all_games_details_goalies') }}
    group by 1, 2
    union
    select
        player_id,
        player_name
    from {{ ref ('stg_all_games_details_skaters') }}
    group by 1, 2
),

player_info as (
    select
        t1.player_id,
        t1.player_firstname,
        t1.player_lastname,
        t2.player_name,
        t1.is_active,
        t1.position,
        t1.birth_country,
        t1.birth_state_province,
        t1.birth_city,
        t1.birthdate,
        t1.draft_year,
        t1.draft_round,
        t1.draft_team_id,
        t1.draft_overall_pick,
        t1.draft_pick_in_round,
        t1.current_team_id,
        t1.team_full_name,
        t1.shoots_catches,
        t1.height_centimeters,
        t1.height_inches,
        t1.weight_kilogram,
        t1.weight_pounds
    from source as t1
    left join players as t2
        on t1.player_id = t2.player_id
    where rn = 1
)

select distinct * from player_info
