{{
  config(
    materialized = 'ephemeral',
    tags = ['nhl', 'staging', 'game_id']
  )
}}

with source as (
    select 
        payload,
        source_filename,
        split_part(source_filename, '_', 2)::int as game_id
    from {{ source('raw', 'nhl_raw_all_games_summary_details') }}
    where split_part(source_filename, '_', 2) is not null
)

select * from source