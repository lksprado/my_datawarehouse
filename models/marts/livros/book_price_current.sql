{{
  config(
    tags = ['livros','marts'],
    )
}}

-- Uma linha por livro com o preço conhecido mais recente e as métricas de
-- oportunidade. É este o modelo para responder "o que vale comprar agora".

WITH
author AS (
    SELECT * FROM {{ ref('dim_authors') }}
),

books AS (
    SELECT * FROM {{ ref('dim_books') }}
),

atual AS (
    SELECT * FROM {{ ref('int_books_price_current') }}
),

final AS (
    SELECT
        t1.book_id,
        t1.author_id,
        t2.author_name,
        t3.book_name,
        t3.book_category,

        t1.last_observed_at,
        t1.reference_date,
        t1.days_since_last_observed,
        t1.is_stale_price,

        t1.book_price_old,
        t1.current_price,
        t1.current_discount,
        t1.prev_price,
        t1.price_change,
        t1.is_price_drop,

        t1.min_price_ever,
        t1.max_price_ever,
        t1.avg_price_ever,
        t1.price_vs_min_ever,
        t1.pct_above_min_ever,
        t1.is_at_record_low,
        t1.is_new_record_low,

        t1.first_observed_at,
        t1.total_observations
    FROM atual AS t1
    LEFT JOIN author AS t2
        ON t1.author_id = t2.author_id
    LEFT JOIN books AS t3
        ON t1.book_id = t3.book_id
)

SELECT * FROM final
ORDER BY pct_above_min_ever ASC, current_price ASC
