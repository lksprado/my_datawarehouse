{{
  config(
    materialized = 'table',
    tags = ['nhl','staging']
    )
}}

with 
source as (
  select * from {{ source('raw','nhl_raw_all_teams_id')}}
),
renamed as (
  select
  (payload ->> 'id')::int as team_id,
  (payload ->> 'triCode') as team_code,
  (payload ->> 'fullName') as team_fullname,
  (payload ->> 'franchiseId')::int as franchise_id
  from source 
  where (payload ->> 'id')::int  <> 70
)
select * from renamed
