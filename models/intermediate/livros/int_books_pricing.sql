{{
  config(
    tags = ['livros','intermediate'],
    )
}}

WITH
legado AS (
    SELECT * FROM {{ ref('stg_vide_all_books_legacy') }}
),

home AS (
    SELECT * FROM {{ ref('stg_vide_home_featured') }}
),

categorias AS (
    SELECT * FROM {{ ref('stg_vide_category_pages') }}
),

unioned AS (
    SELECT
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount
    FROM legado
    UNION ALL
    SELECT
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount
    FROM home
    UNION ALL
    SELECT
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount
    FROM categorias
),

dedup AS (
    SELECT *
    FROM (
        SELECT
            created_at,
            book_id,
            author_id,
            book_price_old,
            book_price_new,
            book_discount,
            ROW_NUMBER() OVER (
                PARTITION BY book_id, book_price_new
                ORDER BY created_at DESC
            ) AS rn_price
        FROM unioned
    ) AS t
    WHERE rn_price = 1
),

final AS (
    SELECT
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount,
        ROW_NUMBER() OVER (
            PARTITION BY book_id
            ORDER BY created_at ASC
        ) AS extraction_order,

        -- última extração de cada livro
        MAX(created_at) OVER (
            PARTITION BY book_id
        ) AS last_extraction_at,

        LAG(book_price_new) OVER (
            PARTITION BY book_id
            ORDER BY created_at ASC
        ) AS prev_price,

        -- maior desconto ANTES do registro atual (sem incluir o current row)
        MAX(book_discount) OVER (
            PARTITION BY book_id
            ORDER BY created_at ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS max_discount_before
    FROM dedup
),

final_with_flags AS (
    SELECT
        *,

        -- só sinaliza queda de preço se for a extração mais recente
        COALESCE(created_at = last_extraction_at
        AND book_price_new IS NOT NULL
        AND prev_price IS NOT NULL
        AND book_price_new < prev_price, FALSE)                      AS is_price_drop,

        COALESCE(book_discount IS NOT NULL
        AND extraction_order > 1
        AND book_discount > COALESCE(max_discount_before, 0), FALSE) AS is_record_discount

    FROM final
)

SELECT * FROM final_with_flags
