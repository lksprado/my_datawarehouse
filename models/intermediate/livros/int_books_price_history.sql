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

-- Grão do histórico: uma observação por livro por dia.
-- As três fontes se sobrepõem; quando divergem no mesmo dia (~1,6% dos pares)
-- mantém o menor preço anunciado, que é o relevante para acompanhar oportunidade.
diario AS (
    SELECT DISTINCT ON (book_id, created_at)
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount
    FROM unioned
    WHERE book_price_new IS NOT NULL
    ORDER BY book_id, created_at, book_price_new ASC
),

com_janelas AS (
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
        ) AS observation_order,

        LAG(book_price_new) OVER (
            PARTITION BY book_id
            ORDER BY created_at ASC
        ) AS prev_price,

        LAG(created_at) OVER (
            PARTITION BY book_id
            ORDER BY created_at ASC
        ) AS prev_observed_at,

        -- menor preço observado ANTES desta linha (não inclui a linha atual)
        MIN(book_price_new) OVER (
            PARTITION BY book_id
            ORDER BY created_at ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS min_price_before,

        -- última observação conhecida do livro
        MAX(created_at) OVER (
            PARTITION BY book_id
        ) AS last_observed_at
    FROM diario
),

final AS (
    SELECT
        created_at,
        book_id,
        author_id,
        book_price_old,
        book_price_new,
        book_discount,
        observation_order,
        prev_price,
        prev_observed_at,
        min_price_before,
        last_observed_at,

        (created_at - prev_observed_at)                           AS days_since_prev_observation,
        (book_price_new - prev_price)                             AS price_change,
        (created_at = last_observed_at)                           AS is_latest_observation,

        COALESCE(book_price_new < prev_price, FALSE)              AS is_price_drop,
        COALESCE(book_price_new > prev_price, FALSE)              AS is_price_increase,

        -- recorde no momento da observação: mais barato que tudo que veio antes
        COALESCE(book_price_new < min_price_before, FALSE)        AS is_record_low
    FROM com_janelas
)

SELECT * FROM final
