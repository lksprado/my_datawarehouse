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
        author_id,
        book_author
    FROM legado
    UNION
    SELECT
        author_id,
        book_author
    FROM home
    UNION
    SELECT
        author_id,
        book_author
    FROM categorias
),

final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY book_author) AS rn
    FROM unioned
)

SELECT
    author_id,
    book_author AS author_name
FROM final
WHERE rn = 1
