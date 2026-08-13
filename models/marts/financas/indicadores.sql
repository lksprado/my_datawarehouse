{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

-- Indexadores macro no grão mensal, expostos em marts.
--
-- Por que existe: `riqueza` só publica os índices ACUMULADOS (base 1), que
-- respondem "o patrimônio cresceu mais que a inflação desde 2023?". Não
-- respondem "quanto foi o IPCA do mês passado?" — e é essa a pergunta do
-- relatório de meio de mês, que roda depois da publicação oficial do índice.
-- As taxas mensais existiam só em `int_indexadores`, e as extrações dos
-- relatórios leem exclusivamente a camada marts.
--
-- Diferença deliberada para `riqueza`: aqui o join é LEFT contra a espinha de
-- meses do próprio stg_patrimonio, então um mês com indexador ainda não
-- preenchido na planilha APARECE com NULL em vez de sumir. `riqueza` faz INNER
-- JOIN e a linha desaparece, o que fazia a série encolher em silêncio. O NULL
-- é o sinal que o portão de prontidão do relatório de meio de mês lê.

WITH indexadores AS (
    SELECT * FROM {{ ref('int_indexadores') }}
),

datas AS (
    SELECT
        date_day           AS mes_base,
        quarter_of_year    AS trimestre,
        year_number        AS ano
    FROM {{ ref('dim_datas') }}
),

final AS (
    SELECT
        t1.mes_base,
        t2.trimestre,
        t2.ano,

        -- Taxas do mês, em fração decimal (0,0042 = 0,42%). Chegam já
        -- divididas por 100 do staging.
        t1.minha_inflacao,
        t1.ipca,
        t1.igpm,
        t1.selic,
        t1.cdi,

        -- Índices acumulados base 1, lidos prontos da planilha.
        t1.minha_inflacao_acum,
        t1.ipca_acum,
        t1.igpm_acum,
        t1.selic_acum,
        t1.cdi_acum
    FROM indexadores AS t1
    LEFT JOIN datas AS t2
        ON t1.mes_base = t2.mes_base
)

SELECT
    final.*,
    CURRENT_TIMESTAMP AS model_updated_at
FROM final
ORDER BY final.mes_base
