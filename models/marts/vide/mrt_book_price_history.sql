{{
  config(
    tags = ['livros','marts'],
    )
}}

with
author as (
    select * from {{ ref('int_dim_author') }}
),

books as (
    select * from {{ ref('int_dim_books') }}
),

prices as (
    select * from {{ ref('int_fct_book_prices') }}
),

final as (
    select
        t1.created_at as date_price,
        t1.book_id,
        t1.author_id,
        t2.author_name,
        t3.book_name,
        t3.book_category,
        t1.book_price_old,
        t1.book_price_new,
        t1.book_discount,
        t1.book_price_new_order
    from prices as t1
    left join author as t2
        on t1.author_id = t2.author_id
    left join books as t3
        on t1.book_id = t3.book_id
    where t1.book_price_new is not null
)

select * from final
order by book_id asc, book_price_new_order asc