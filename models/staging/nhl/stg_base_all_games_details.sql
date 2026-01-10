-- models/staging/nhl/base/base_nhl_raw_all_games_details.sql
{{
  config(
    materialized = 'ephemeral',
    tags = ['nhl', 'staging']
  )
}}

with source as (
    select payload
    from {{ source('raw', 'nhl_raw_all_games_details') }}
),

base_fields as (
    select
        payload,
        -- Campos comuns já parseados
        (payload ->> 'id')::int as game_id,
        (payload ->> 'season')::int as season_id,
        (payload ->> 'gameType')::int as game_type_id,
        (payload ->> 'gameDate')::date as game_date,
        (payload ->> 'gameState') as game_state
    from source
    where (payload ->> 'id') is not null
)

select * from base_fields