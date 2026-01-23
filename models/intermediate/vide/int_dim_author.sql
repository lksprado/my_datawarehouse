{{
  config(
    tags = ['livros','intermediate'],
    )
}}

with
legado as (
    select * from {{ ref('stg_vide_all_books_legacy') }}
),

home as (
    select * from {{ ref('stg_vide_home_featured') }}
),

categorias as (
    select * from {{ ref('stg_vide_category_pages') }}
),

unioned as (
    select
        author_id,
        book_author
    from legado
    union
    select
        author_id,
        book_author
    from home
    union
    select
        author_id,
        book_author
    from categorias
),

final as (
    select
        *,
        row_number() over (partition by author_id order by book_author) as rn
    from unioned
)

select
    author_id,
    book_author as author_name
from final
where rn = 1