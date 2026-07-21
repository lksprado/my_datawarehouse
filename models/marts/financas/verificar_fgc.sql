


with 
check as (
    SELECT DISTINCT
    t1.emissor, t2.conglomerado
    FROM {{ ref('int_renda_fixa') }} t1
    LEFT JOIN {{ ref('stg_de_para_instituicoes_fgc') }} t2
    ON t1.emissor = t2.instituicao 
    WHERE t1.emissor IS NOT NULL AND t1.emissor <> ''
    AND split_part(upper(t1.investimento), ' ', 1)
    IN ('CDB', 'LCA', 'LCI', 'LC')
    AND t2.conglomerado IS NULL;
)
select * from check