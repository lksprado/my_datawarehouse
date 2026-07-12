{{
  config(
    materialized = 'table',
    tags = ['livros', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw', 'vide_raw_all_books_legacy') }}
),

renamed AS (
    SELECT
        book_name,
        book_author,
        book_category,
        TRIM(REPLACE(REPLACE(REPLACE(book_price_old, 'R$ ', ''), '.', ''), ',', '.'))::NUMERIC(10, 2) AS book_price_old,
        TRIM(REPLACE(REPLACE(REPLACE(book_price_new, 'R$ ', ''), '.', ''), ',', '.'))::NUMERIC(10, 2) AS book_price_new,
        REGEXP_REPLACE(public.unaccent(LOWER(book_name)), '[^a-z0-9]', '', 'g')                       AS book_name_clean,
        REGEXP_REPLACE(public.unaccent(LOWER(book_author)), '[^a-z0-9]', '', 'g')                     AS book_author_clean,
        TO_DATE(time, 'YYYY-MM-DD HH24:MI:SS')                                                        AS created_at,
        CASE
            WHEN book_category = 'Filósofos Brasileiros' THEN 'Filosofia'
            WHEN book_category = 'Literatura Estrangeira' THEN 'Filosofia'
            WHEN book_category = 'História do Brasil' THEN 'História'
            WHEN book_category = 'Filósofos' THEN 'Filosofia'
            WHEN book_category = 'Filosofia da História' THEN 'Filosofia'
            WHEN book_category = 'Filosofia Política' THEN 'Política'
            WHEN book_category = 'Ciências Sociais' THEN 'Ciências Sociais'
            WHEN book_category = 'Filosofia' THEN 'Filosofia'
            WHEN book_category = 'Filosofia Moderna e Contemporânea' THEN 'Filosofia'
            WHEN book_category = 'Literatura Brasileira' THEN 'Literatura'
            WHEN book_category = 'Lógica e Dialética' THEN 'Filosofia'
            WHEN book_category = 'Ensaios e Estudos Filosóficos' THEN 'Filosofia'
            WHEN book_category = 'Oratória e Retórica' THEN 'Filosofia'
            WHEN book_category = 'Ética e Filosofia Moral' THEN 'Filosofia'
            WHEN book_category = 'Literatura' THEN 'Literatura'
            WHEN book_category = 'Metafísica' THEN 'Filosofia'
            WHEN book_category = 'Biografias' THEN 'Biografias'
            WHEN book_category = 'História da Filosofia' THEN 'Filosofia'
            WHEN book_category = 'Auto-Ajuda' THEN 'Autoconhecimento'
            WHEN book_category = 'Introdução à Filosofia' THEN 'Filosofia'
            WHEN book_category = 'Ensino e estudo de línguas' THEN 'Filosofia'
            WHEN book_category = 'Literatura Portuguesa' THEN 'Literatura'
            WHEN book_category = 'Autoconhecimento' THEN 'Autoconhecimento'
            WHEN book_category = 'Antropologia' THEN 'Ciências Sociais'
            WHEN book_category = 'Filosofia Antiga' THEN 'Filosofia'
            WHEN book_category = 'História' THEN 'História'
            WHEN book_category = 'História da América Latina' THEN 'História'
            WHEN book_category = 'Sociologia' THEN 'Ciências Sociais'
        END                                                                                           AS category
    FROM source
    WHERE book_category IN (
        'Filósofos Brasileiros',
        'Literatura Estrangeira',
        'História do Brasil',
        'Filósofos',
        'Filosofia da História',
        'Filosofia Política',
        'Ciências Sociais',
        'Filosofia',
        'Filosofia Moderna e Contemporânea',
        'Literatura Brasileira',
        'Lógica e Dialética',
        'Ensaios e Estudos Filosóficos',
        'Oratória e Retórica',
        'Ética e Filosofia Moral',
        'Literatura',
        'Metafísica',
        'Biografias',
        'História da Filosofia',
        'Auto-Ajuda',
        'Introdução à Filosofia',
        'Literatura Portuguesa',
        'Linguística',
        'Autoconhecimento',
        'Antropologia',
        'Filosofia Antiga',
        'História',
        'História da América Latina',
        'Sociologia'
    )
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['book_name_clean', 'book_author_clean']) }}            AS book_id,
        {{ dbt_utils.generate_surrogate_key(['book_author_clean']) }}                               AS author_id,
        REPLACE(REGEXP_REPLACE(public.unaccent(LOWER(book_name)), '[^a-z0-9 ]', '', 'g'), '  ', ' ') AS book_name,
        LOWER(book_author)                                                                          AS book_author,
        category                                                                                    AS book_category,
        book_price_old,
        book_price_new,
        ((book_price_new - book_price_old) / book_price_old)::NUMERIC(6, 2)                         AS book_discount,
        created_at
    FROM renamed
)

SELECT * FROM final
