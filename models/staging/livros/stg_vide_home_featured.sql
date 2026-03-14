{{
  config(
    materialized = 'table',
    tags = ['livros', 'staging'],
  )
}}

with
source as (
    select * from {{ source('raw', 'vide_raw_home_featured') }}
),

renamed as (
    select
        name as book_name,
        author_name as book_author,
        trim(replace(replace(replace(price_old, 'R$ ', ''), '.', ''), ',', '.'))::numeric(10, 2) as book_price_old,
        trim(replace(replace(replace(price_new, 'R$ ', ''), '.', ''), ',', '.'))::numeric(10, 2) as book_price_new,
        created_at::date as created_at,
        regexp_replace(public.unaccent(lower(name)), '[^a-z0-9]', '', 'g') as book_name_clean,
        regexp_replace(public.unaccent(lower(author_name)), '[^a-z0-9]', '', 'g') as book_author_clean
    from source
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['book_name_clean', 'book_author_clean']) }} as book_id,
        {{ dbt_utils.generate_surrogate_key(['book_author_clean']) }} as author_id,
        replace(regexp_replace(public.unaccent(lower(book_name)), '[^a-z0-9 ]', '', 'g'), '  ',' ') as book_name,
        lower(book_author) as book_author,
        book_price_old,
        book_price_new,
        ((book_price_new - book_price_old) / book_price_old)::numeric(6, 2) as book_discount,
        created_at
    from renamed
)

select * from final