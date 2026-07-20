{{
  config(
    tags = ['livros','intermediate'],
    )
}}

-- Estado atual de cada livro: o último preço CONHECIDO, venha ele da coleta de
-- hoje ou da última varredura completa. A coleta alterna entre ~30 destaques
-- diários e varreduras do catálogo inteiro, então ancorar em max(created_at)
-- global enxergaria menos de 1% dos livros.

WITH
historico AS (
    SELECT * FROM {{ ref('int_books_price_history') }}
),

ultima_observacao AS (
    SELECT *
    FROM historico
    WHERE is_latest_observation
),

agregados AS (
    SELECT
        book_id,
        MIN(book_price_new)              AS min_price_ever,
        MAX(book_price_new)              AS max_price_ever,
        AVG(book_price_new)::NUMERIC(10, 2) AS avg_price_ever,
        MIN(created_at)                  AS first_observed_at,
        COUNT(*)                         AS total_observations
    FROM historico
    GROUP BY book_id
),

-- data mais recente do dataset, usada como "hoje" para medir defasagem
referencia AS (
    SELECT MAX(created_at) AS reference_date
    FROM historico
),

final AS (
    SELECT
        t1.book_id,
        t1.author_id,
        t1.created_at                    AS last_observed_at,
        t3.reference_date,
        (t3.reference_date - t1.created_at) AS days_since_last_observed,

        t1.book_price_old,
        t1.book_price_new                AS current_price,
        t1.book_discount                 AS current_discount,
        t1.prev_price,
        t1.price_change,
        t1.is_price_drop,

        t2.min_price_ever,
        t2.max_price_ever,
        t2.avg_price_ever,
        t2.first_observed_at,
        t2.total_observations,

        (t1.book_price_new - t2.min_price_ever) AS price_vs_min_ever,
        CASE
            WHEN t2.min_price_ever > 0
                THEN ROUND(100.0 * (t1.book_price_new - t2.min_price_ever) / t2.min_price_ever, 2)
        END                              AS pct_above_min_ever,

        -- oportunidade: o preço conhecido hoje empata ou bate o mínimo histórico
        (t1.book_price_new <= t2.min_price_ever) AS is_at_record_low,

        -- o recorde foi estabelecido justamente na última observação
        t1.is_record_low                 AS is_new_record_low,

        -- preço defasado: livro não é observado há mais de uma semana
        ((t3.reference_date - t1.created_at) > 7) AS is_stale_price
    FROM ultima_observacao AS t1
    INNER JOIN agregados AS t2
        ON t1.book_id = t2.book_id
    CROSS JOIN referencia AS t3
)

SELECT * FROM final
