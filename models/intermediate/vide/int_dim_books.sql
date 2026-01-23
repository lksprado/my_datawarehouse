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
        book_id,
        book_name,
        book_category
    from legado
    union all
    select
        book_id,
        book_name,
        null as book_category
    from home
    union all
    select
        book_id,
        book_name,
        book_category
    from categorias
    order by book_category
),

-- conta quantas vezes cada categoria aparece por livro
category_counts as (
    select
        book_id,
        book_name,
        book_category,
        count(*) as category_count
    from unioned
    where book_category is not null
    group by
        book_id,
        book_name,
        book_category
),

-- rankeia categorias por frequência (e desempata de forma estável)
ranked as (
    select
        *,
        row_number() over (
            partition by book_id
            order by
                category_count desc,      -- mais frequente primeiro
                book_category asc         -- desempate determinístico
        ) as rn
    from category_counts
)

select
    book_id,
    book_name,
    book_category
from ranked
where rn = 1