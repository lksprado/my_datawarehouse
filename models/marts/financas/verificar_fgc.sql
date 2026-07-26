{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}


{# SE CONSTAR DADOS NESTA SAÍDA, ATUALIZE AS INSTITUIÇOES DO FGC VIA CLAUDE #}

WITH
tb AS (
    SELECT DISTINCT
        t1.emissor,
        t2.conglomerado
    FROM {{ ref('int_renda_fixa') }} AS t1
    LEFT JOIN {{ ref('stg_de_para_instituicoes_fgc') }} AS t2
        ON t1.emissor = t2.instituicao
    WHERE
        t1.emissor IS NOT NULL
        AND t1.emissor <> ''
        AND SPLIT_PART(UPPER(t1.ativo), ' ', 1) IN ('CDB', 'LCA', 'LCI', 'LC')
        AND t2.conglomerado IS NULL
)

SELECT * FROM tb
