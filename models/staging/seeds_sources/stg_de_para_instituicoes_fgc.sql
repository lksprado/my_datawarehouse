{{
  config(
    materialized = 'table',
    tags = ['financas', 'staging'],
  )
}}

WITH
seed AS (
    SELECT
    data_extracao::DATE as data_extracao,
    {{ clean_string("nome_conglomerado", "upper") }} as conglomerado,
    {{ clean_string("nome_instituicao", "upper") }} as instituicao    
    FROM {{ ref('de_para_instituicoes_fgc') }}
    WHERE score_match > 90
),

renamed as (
    SELECT
      data_extracao,
      TRIM(
          REGEXP_REPLACE(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      conglomerado,
                      '\s+S(?:\.|/)?A\.?\s*$',
                      '',
                      'i'
                  ),
                  '[[:punct:]]+',
                  ' ',
                  'g'
              ),
              '\s+',
              ' ',
              'g'
          )
      ) as conglomerado,
      TRIM(
          REGEXP_REPLACE(
              REGEXP_REPLACE(
                  REGEXP_REPLACE(
                      instituicao,
                      '\s+S(?:\.|/)?A\.?\s*$',
                      '',
                      'i'
                  ),
                  '[[:punct:]]+',
                  ' ',
                  'g'
              ),
              '\s+',
              ' ',
              'g'
          )
      ) as instituicao
    FROM seed
),
renamed_2 AS (
    SELECT DISTINCT
    CASE 
        WHEN instituicao LIKE 'NU FINANCEIRA SA SOCIEDADE DE CREDITO FINANCIAMENTO E INVESTIMENTO' THEN 'NUBANK'
        WHEN instituicao LIKE 'NU FINANCEIRA S A SOCIEDADE DE CREDITO FINANCIAMENTO E INVESTIMENTO' THEN 'NUBANK'
        WHEN instituicao LIKE 'NU FINANCEIRA SA SOCIEDADE CFI' THEN 'NUBANK'
        ELSE instituicao
    END AS instituicao,
    CASE 
        WHEN conglomerado LIKE 'NU FINANCEIRA SA SOCIEDADE DE CREDITO FINANCIAMENTO E INVESTIMENTO' THEN 'NUBANK'
        WHEN conglomerado LIKE 'NU FINANCEIRA S A SOCIEDADE DE CREDITO FINANCIAMENTO E INVESTIMENTO' THEN 'NUBANK'
        ELSE conglomerado
    END AS conglomerado
    FROM renamed
),
filler as (
    SELECT
    'BANCO MASTER' AS conglomerado,
    'BANCO MASTER' AS instituicao
)
SELECT * FROM renamed_2 
UNION ALL
SELECT * FROM filler