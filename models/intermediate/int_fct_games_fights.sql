{{
  config(
    materialized = 'view',
    tags = ['nhl','intermediate'],
    )
}}

with
players as (
    select
    player_id,
    player_name,
    game_id,
    team_id
    from {{ ref('int_eph_players_games') }}
),
teams as (
    select * from {{ ref('int_dim_teams') }}
),
games as (
    select * from {{ ref('int_dim_games') }}
    where season_id >= 19851986
),
hf as (
    select
        fight_id,
        season_id,
        game_type_id,
        player_1_name,
        player_2_name,
        team_1_code,
        player_1_team,
        substring(player_1_team from position(' ' in player_1_team) + 1) AS team_1_commoname,
        team_2_code,
        player_2_team,        
        substring(player_2_team from position(' ' in player_2_team) + 1) AS team_2_commoname,
        game_date,
        period,
        time_in_period,
        fight_winner,
        rating,
        vote_count
    from {{ ref('stg_all_hockeyfights') }}
),
team as (
    select distinct
    fight_id,
    COALESCE(t2.team_id, t4.team_id) as team_1_id,
    COALESCE(t3.team_id, t5.team_id) as team_2_id
    from hf t1
    left join teams t2 
    on t1.team_1_code = t2.team_code and t1.season_id >= t2.first_season_id
    left join teams t3 
    on t1.team_2_code = t3.team_code and t1.season_id >= t3.first_season_id
    left join teams t4 
    on t1.team_1_commoname = t4.team_commonname and t1.season_id >= t4.first_season_id
    left join teams t5 
    on t1.team_2_commoname = t5.team_commonname and t1.season_id >= t5.first_season_id
),
hf_with_teams as (
    select 
    t1.fight_id,
    t1.season_id,
    t1.game_type_id,
    t1.player_1_name,
    t1.player_2_name,
    t2.team_1_id,
    t2.team_2_id,
    t1.game_date,
    t1.period,
    t1.time_in_period,
    t1.fight_winner,
    t1.rating,
    t1.vote_count
    from hf t1 
    left join team t2 
    on t1.fight_id = t2.fight_id
),
hf_with_ha_flag as (
    select 
        t1.fight_id,
        t1.season_id,
        t1.player_1_name,
        t1.player_2_name,
        case 
            when t1.team_1_id = t2.home_team_id then t1.team_1_id 
            else t1.team_2_id
        end as home_team_id,
        case 
            when t1.team_1_id = t2.home_team_id then t1.team_2_id
            else t1.team_1_id
        end as away_team_id,
        t1.game_date,
        t1.period,
        t1.time_in_period,
        t1.fight_winner,
        t1.rating,
        t1.vote_count
    from hf_with_teams t1 
    left join games t2 
        on t1.season_id = t2.season_id
        and t1.game_type_id = t2.game_type_id
        and t1.game_date = t2.game_date
),
final as (
    select 
    distinct
    t1.fight_id,
    t2.game_id,
    concat(t1.player_1_name, ' vs. ',t1.player_2_name) as fight,    
    t3.player_id as player_id_1,
    t4.player_id as player_id_2,
    t1.period,
    t1.time_in_period,
    t1.fight_winner,
    t1.rating,
    t1.vote_count
    from hf_with_ha_flag t1
    left join games t2 
    on t1.season_id = t2.season_id
    and t1.game_date = t2.game_date
    and t1.home_team_id = t2.home_team_id
    and t1.away_team_id = t2.away_team_id
    left join players t3 
    on t1.player_1_name = t3.player_name and t2.game_id = t3.game_id and t1.home_team_id = t3.team_id
    left join players t4 
    on t1.player_2_name = t4.player_name and t2.game_id = t4.game_id and t1.away_team_id = t4.team_id
    where t2.game_id is not null
)
select * from final order by game_id desc

