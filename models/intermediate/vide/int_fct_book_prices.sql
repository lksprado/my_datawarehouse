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
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount
    from legado
    union all
    select
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount
    from home
    union all
    select
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount
    from categorias
),

dedup as (
    select *
    from (
        select
            created_at,
            book_id,
            author_id,
            book_price_old,
            book_price_new,
            book_discount,
            row_number() over (
                partition by book_id, book_price_new
                order by created_at desc
            ) as rn_price
        from unioned
    ) as t
    where rn_price = 1
),

final as (
    select
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount,
        row_number() over (
            partition by book_id
            order by created_at asc
        ) as book_price_new_order
    from dedup
)

select * from final
order by created_at desc, book_id desc