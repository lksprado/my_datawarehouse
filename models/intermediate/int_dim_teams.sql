{{
  config(
    tags = ['nhl','intermediate'],
    )
}}

with source as (
    select *
    from {{ ref('stg_all_teams') }}
),
game_details as (
    select 
    home_team_id as team_id,
    string_agg(distinct home_team_logo, ', ' order by home_team_logo) as team_logo,
    string_agg(distinct home_team_darklogo, ', ' order by home_team_darklogo) as team_darklogo,
    string_agg(distinct home_team_placename, ', ' order by home_team_placename) as team_placename,
    string_agg(distinct home_team_commonname, ', ' order by home_team_commonname) as team_commonname,
    max(season_id) as latest_season_id
    from {{ ref('stg_all_games_details') }}
    group by home_team_id
),
seasons as (
    select * from {{ ref('vw_stg_request_seasons_id')}}
),
teams as (
    select
        t1.team_id,
        t1.team_code,
        t1.team_fullname,
        t2.team_placename,
        t2.team_commonname,
        t2.latest_season_id,
        t3.is_current as is_active
        t2.team_logo,
        t2.team_darklogo,

    from source t1
    left join game_details t2
    on t1.team_id = t2.team_id
    left join seasons t3
    on t2.latest_season_id = t3.season_id
    where t1.team_id < 99
)

select * from teams
