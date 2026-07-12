{{
  config(
    tags = ['livros','marts'],
    )
}}

WITH
author AS (
    SELECT * FROM {{ ref('int_dim_authors') }}
),

books AS (
    SELECT * FROM {{ ref('int_dim_books') }}
),

prices AS (
    SELECT * FROM {{ ref('int_fct_book_prices') }}
),

final AS (
    SELECT
        t1.created_at AS date_price,
        t1.book_id,
        t1.author_id,
        t2.author_name,
        t3.book_name,
        t3.book_category,
        t1.book_price_old,
        t1.book_price_new,
        t1.book_discount,
        t1.extraction_order,
        t1.prev_price,
        t1.is_price_drop,
        t1.is_record_discount
    FROM prices AS t1
    LEFT JOIN author AS t2
        ON t1.author_id = t2.author_id
    LEFT JOIN books AS t3
        ON t1.book_id = t3.book_id
    WHERE t1.book_price_new IS NOT NULL
)

SELECT * FROM final
ORDER BY book_id ASC, extraction_order ASC
