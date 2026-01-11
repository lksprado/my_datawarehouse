{{
  config(
    tags = ['nhl','intermediate'],
    )
}}


with source as (
    select *,
          row_number() over (
            partition by player_id
          ) as rn
    from {{ ref('stg_all_players') }}
),

player_info as (
    select
        player_id,
        is_active,
        position,
        birth_country,
        birth_state_province,
        birth_city,
        birthdate,
        draft_year,
        draft_round,
        draft_team_id,
        draft_overall_pick,
        draft_pick_in_round,
        current_team_id,
        team_full_name,
        shoots_catches,
        height_centimeters,
        height_inches,
        weight_kilogram,
        weight_pounds
    from source
    where rn = 1
)

select * from player_info
