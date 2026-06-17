{{
  config(
    tags = ['livros','intermediate'],
    )
}}

with
legado as (
    select * from {{ ref('stg_vide_all_books_legacy') }}
    where book_name is not null
),

home as (
    select * from {{ ref('stg_vide_home_featured') }}
    where book_name is not null
),

categorias as (
    select * from {{ ref('stg_vide_category_pages') }}
    where book_name is not null
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
),
unioned_dedup as (
    select 
        book_id,
        book_name
    from unioned 
    group by 1,2
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
    group by 1,2,3
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
    d.book_id,
    d.book_name,
    r.book_category    -- NULL para livros sem categoria
from unioned_dedup d
left join ranked r
    on r.book_id = d.book_id
    and r.rn = 1