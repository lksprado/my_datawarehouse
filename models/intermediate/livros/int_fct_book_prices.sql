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
        ) as extraction_order,

        -- última extração de cada livro
        max(created_at) over (
            partition by book_id
        ) as last_extraction_at,

        lag(book_price_new) over (
            partition by book_id
            order by created_at asc
        ) as prev_price,

        -- maior desconto ANTES do registro atual (sem incluir o current row)
        max(book_discount) over (
            partition by book_id
            order by created_at asc
            rows between unbounded preceding and 1 preceding
        ) as max_discount_before
    from dedup
),

final_with_flags as (
    select
        *,

        -- só sinaliza queda de preço se for a extração mais recente
        case
            when created_at = last_extraction_at
             and book_price_new is not null
             and prev_price is not null
             and book_price_new < prev_price
            then true
            else false
        end as is_price_drop,

        case
            when book_discount is not null
            and extraction_order > 1
            and book_discount > coalesce(max_discount_before, 0)
            then true
            else false
        end as is_record_discount

    from final
)

select * from final_with_flags
