{{
  config(
    tags = ['livros','marts'],
    )
}}

WITH
books AS (
    SELECT * FROM {{ ref('int_books_treated') }}
)
select * from books