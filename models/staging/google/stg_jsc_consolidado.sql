{{
  config(
    tags = ['investimentos', 'staging'],
  )
}}

WITH
source AS (
    SELECT * FROM {{ source('raw','jsc_consolidado') }}
),

renamed AS (
    SELECT
        REGEXP_REPLACE(salario, '[^0-9]', '', 'g')::INT        AS salario,
        REGEXP_REPLACE(dividendos, '[^0-9]', '', 'g')::INT     AS dividendos,
        REGEXP_REPLACE(outros, '[^0-9]', '', 'g')::INT         AS outros,
        REGEXP_REPLACE(receita_total, '[^0-9]', '', 'g')::INT  AS receita_total,
        REGEXP_REPLACE(t_mercado, '[^0-9]', '', 'g')::INT      AS mercado,
        REGEXP_REPLACE(t_diversos, '[^0-9]', '', 'g')::INT     AS diversos,
        REGEXP_REPLACE(t_assinaturas, '[^0-9]', '', 'g')::INT  AS assinaturas,
        REGEXP_REPLACE(t_role, '[^0-9]', '', 'g')::INT         AS role,
        REGEXP_REPLACE(t_transporte, '[^0-9]', '', 'g')::INT   AS transporte,
        REGEXP_REPLACE(t_apartamento, '[^0-9]', '', 'g')::INT  AS apartamento,
        REGEXP_REPLACE(t_saude, '[^0-9]', '', 'g')::INT        AS saude,
        REGEXP_REPLACE(t_educacao, '[^0-9]', '', 'g')::INT     AS educacao,
        REGEXP_REPLACE(despesas_total, '[^0-9]', '', 'g')::INT AS despesas_total,
        REGEXP_REPLACE(resultado, '[^0-9]', '', 'g')::INT      AS resultado,
        'jessica'                                              AS pessoa,
        TO_DATE("mes.crd", 'MM-YYYY')                          AS mes_fatura,
        TO_DATE("mes.deb", 'MM-YYYY')                          AS mes_debito
    FROM source
)

SELECT * FROM renamed
