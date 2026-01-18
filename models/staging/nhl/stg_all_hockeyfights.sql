{{
  config(
    materialized = 'table',
    tags = ['nhl','staging', 'player_id']
    )
}}

with
source as (
    select * from {{ source('raw','hockeyfights_raw_all_fights') }}
),

renamed as (
    select
        replace(season, '-', '')::int as season_id,
        case
            when season_type like 'reg' then 2
            when season_type like 'pos' then 3
        end as game_type_id,
        {{ dbt_utils.generate_surrogate_key(['fight', 'date', 'gametime']) }} as fight_id,
        player_1_name,
        player_2_name,
        player_1_team,
        player_2_team,
        split_part(split_part(fight, '(', 2), ')', 1) as team_1_id,
        split_part(split_part(fight, '(', 3), ')', 1) as team_2_id,
        to_date(date, 'MM/DD/YY') as game_date,
        period,
        gametime as time_in_period,
        winner as fight_winner,
        rating::float as rating,
        vote_count::int as vote_count
    from source
),
dedup as (
    select
    row_number() over (partition by fight_id) as rn,
    *
    from renamed
),
final as (
    select 
        fight_id,
        season_id,
        game_type_id,
        player_1_name,
        player_2_name,
        player_1_team,
        player_2_team,
        team_1_id,
        team_2_id,
        game_date,
        period,
        time_in_period,
        fight_winner,
        rating,
        vote_count
    from dedup 
    where rn = 1
)
select * from final