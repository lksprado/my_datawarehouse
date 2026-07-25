{{
  config(
    tags = ['financas', 'intermediate'],
  )
}}


WITH
lucas AS (
    SELECT * FROM {{ ref('stg_luc_consolidado') }}
),

jessica AS (
    SELECT * FROM {{ ref('stg_jsc_consolidado') }}
),

ajuste AS (
    SELECT * FROM {{ ref('stg_ajuste') }}
),

unioned AS (
    SELECT * FROM lucas
    UNION ALL
    SELECT * FROM jessica
),

joined AS (
    SELECT 
        u.salario,
        u.dividendos,
        u.outros,
        u.receita_total,
        COALESCE(a.ajuste_realizado, 0) AS ajuste_realizado,
        u.mercado,
        u.diversos,
        u.assinaturas,
        u.role,
        u.transporte,
        u.apartamento,
        u.saude,
        u.educacao,
        u.despesas_total,
        u.resultado,
        u.pessoa,
        u.mes_fatura,
        u.mes_debito  
    
    FROM unioned u
    LEFT JOIN ajuste a 
    ON u.mes_debito = a.mes
    AND u.pessoa = a.pessoa
)

SELECT * FROM joined
ORDER BY
    mes_debito, pessoa
