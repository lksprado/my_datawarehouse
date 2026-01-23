{{
  config(
    materialized = 'table',
    tags = ['livros', 'staging'],
  )
}}

with
source as (
    select * from {{ source('raw', 'vide_raw_all_books_legacy') }}
),

renamed as (
    select
        book_name,
        book_author,
        book_category,
        trim(replace(replace(replace(book_price_old, 'R$ ', ''), '.', ''), ',', '.'))::numeric(10, 2) as book_price_old,
        trim(replace(replace(replace(book_price_new, 'R$ ', ''), '.', ''), ',', '.'))::numeric(10, 2) as book_price_new,
        regexp_replace(public.unaccent(lower(book_name)), '[^a-z0-9]', '', 'g') as book_name_clean,
        regexp_replace(public.unaccent(lower(book_author)), '[^a-z0-9]', '', 'g') as book_author_clean,
        to_date(time, 'YYYY-MM-DD HH24:MI:SS') as created_at,
        case
            when book_category = 'Filósofos Brasileiros' then 'Filosofia'
            when book_category = 'Literatura Estrangeira' then 'Filosofia'
            when book_category = 'História do Brasil' then 'História'
            when book_category = 'Filósofos' then 'Filosofia'
            when book_category = 'Filosofia da História' then 'Filosofia'
            when book_category = 'Filosofia Política' then 'Política'
            when book_category = 'Ciências Sociais' then 'Ciências Sociais'
            when book_category = 'Filosofia' then 'Filosofia'
            when book_category = 'Filosofia Moderna e Contemporânea' then 'Filosofia'
            when book_category = 'Literatura Brasileira' then 'Literatura'
            when book_category = 'Lógica e Dialética' then 'Filosofia'
            when book_category = 'Ensaios e Estudos Filosóficos' then 'Filosofia'
            when book_category = 'Oratória e Retórica' then 'Filosofia'
            when book_category = 'Ética e Filosofia Moral' then 'Filosofia'
            when book_category = 'Literatura' then 'Literatura'
            when book_category = 'Metafísica' then 'Filosofia'
            when book_category = 'Biografias' then 'Biografias'
            when book_category = 'História da Filosofia' then 'Filosofia'
            when book_category = 'Auto-Ajuda' then 'Autoconhecimento'
            when book_category = 'Introdução à Filosofia' then 'Filosofia'
            when book_category = 'Ensino e estudo de línguas' then 'Filosofia'
            when book_category = 'Literatura Portuguesa' then 'Literatura'
            when book_category = 'Autoconhecimento' then 'Autoconhecimento'
            when book_category = 'Antropologia' then 'Ciências Sociais'
            when book_category = 'Filosofia Antiga' then 'Filosofia'
            when book_category = 'História' then 'História'
            when book_category = 'História da América Latina' then 'História'
            when book_category = 'Sociologia' then 'Ciências Sociais'
        end as category
    from source
    where book_category in (
        'Filósofos Brasileiros',
        'Literatura Estrangeira',
        'História do Brasil',
        'Filósofos',
        'Filosofia da História',
        'Filosofia Política',
        'Ciências Sociais',
        'Filosofia',
        'Filosofia Moderna e Contemporânea',
        'Literatura Brasileira',
        'Lógica e Dialética',
        'Ensaios e Estudos Filosóficos',
        'Oratória e Retórica',
        'Ética e Filosofia Moral',
        'Literatura',
        'Metafísica',
        'Biografias',
        'História da Filosofia',
        'Auto-Ajuda',
        'Introdução à Filosofia',
        'Literatura Portuguesa',
        'Linguística',
        'Autoconhecimento',
        'Antropologia',
        'Filosofia Antiga',
        'História',
        'História da América Latina',
        'Sociologia'
    )
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['book_name_clean', 'book_author_clean']) }} as book_id,
        {{ dbt_utils.generate_surrogate_key(['book_author_clean']) }} as author_id,
        book_name,
        book_author,
        category as book_category,
        book_price_old,
        book_price_new,
        ((book_price_new - book_price_old) / book_price_old)::numeric(6, 2) as book_discount,
        created_at
    from renamed
)

select * from final


