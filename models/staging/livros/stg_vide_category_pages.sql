{{
  config(
    materialized = 'table',
    tags = ['livros', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw', 'vide_raw_category_pages') }}
),

renamed AS (
    SELECT
        name                                                                                     AS book_name,
        author_name                                                                              AS book_author,
        TRIM(REPLACE(REPLACE(REPLACE(price_old, 'R$ ', ''), '.', ''), ',', '.'))::NUMERIC(10, 2) AS book_price_old,
        TRIM(REPLACE(REPLACE(REPLACE(price_new, 'R$ ', ''), '.', ''), ',', '.'))::NUMERIC(10, 2) AS book_price_new,
        created_at::DATE                                                                         AS created_at,
        REGEXP_REPLACE(public.unaccent(LOWER(name)), '[^a-z0-9]', '', 'g')                       AS book_name_clean,
        REGEXP_REPLACE(public.unaccent(LOWER(author_name)), '[^a-z0-9]', '', 'g')
            AS book_author_clean,
        CASE
            WHEN source_filename LIKE '%historia%' THEN 'História'
            WHEN source_filename LIKE '%ciencias_sociais%' THEN 'Ciências Sociais'
            WHEN source_filename LIKE '%filosofia%' THEN 'Filosofia'
            WHEN source_filename LIKE '%biografias%' THEN 'Biografias'
            WHEN source_filename LIKE '%literatura%' THEN 'Literatura'
            WHEN source_filename LIKE '%autoconhecimento%' THEN 'Autoconhecimento'
            WHEN source_filename LIKE '%economia%' THEN 'Economia'
            WHEN source_filename LIKE '%politica%' THEN 'Política'
        END                                                                                      AS book_category
    FROM source
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['book_name_clean', 'book_author_clean']) }}            AS book_id,
        {{ dbt_utils.generate_surrogate_key(['book_author_clean']) }}                               AS author_id,
        REPLACE(REGEXP_REPLACE(public.unaccent(LOWER(book_name)), '[^a-z0-9 ]', '', 'g'), '  ', ' ') AS book_name,
        LOWER(book_author)                                                                          AS book_author,
        book_category,
        book_price_old,
        book_price_new,
        ((book_price_new - book_price_old) / book_price_old)::NUMERIC(6, 2)                         AS book_discount,
        created_at
    FROM renamed
)

SELECT * FROM final
