{{
  config(
    tags = ['livros','marts'],
    )
}}

WITH
author AS (
    SELECT * FROM {{ ref('int_authors_treated') }}
)
select * from author