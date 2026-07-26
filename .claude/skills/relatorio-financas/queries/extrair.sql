-- Extração determinística do domínio FINANÇAS para os relatórios mensais.
-- Emite um único registro JSON com todos os blocos que o relatório consome.
--
-- Parâmetro:
--   :mes_ref  -- primeiro dia do mês de referência, ex.: '2026-07-01'
--
-- Regra de mês: o DRE (resultado/consumo) contém meses FUTUROS pré-lançados
-- com despesas fixas recorrentes. Por isso o mês de referência é sempre
-- explícito e nunca derivado de MAX(). A carteira fecha em cadência própria e
-- pode estar defasada em relação ao DRE — `meta.defasagem_carteira_meses`
-- reporta isso para que o relatório declare a data de cada número.

\set ON_ERROR_STOP on

WITH
params AS (
    SELECT :'mes_ref'::date AS mes_ref
),

-- Último mês de carteira disponível que não ultrapassa o mês de referência.
mes_carteira AS (
    SELECT COALESCE(MAX(mes_base), (SELECT mes_ref FROM params)) AS mes_carteira
    FROM (
        SELECT mes_base FROM marts.carteira_lucas_jessica
        UNION ALL
        SELECT mes_base FROM marts.carteira_deusa
    ) AS c
    WHERE c.mes_base <= (SELECT mes_ref FROM params)
),

carteira AS (
    SELECT * FROM marts.carteira_lucas_jessica
    UNION ALL
    SELECT * FROM marts.carteira_deusa
),

carteira_ref AS (
    SELECT c.*
    FROM carteira AS c
    CROSS JOIN mes_carteira AS m
    WHERE c.mes_base = m.mes_carteira
),

total_por_pessoa AS (
    SELECT pessoa, SUM(vlr_atualizado_brl) AS total
    FROM carteira_ref
    GROUP BY pessoa
),

-- ---------------------------------------------------------------- blocos ---

b_meta AS (
    SELECT json_build_object(
        'mes_ref',                    (SELECT mes_ref FROM params),
        'mes_carteira',               (SELECT mes_carteira FROM mes_carteira),
        'defasagem_carteira_meses',   (
            SELECT (EXTRACT(YEAR FROM age(p.mes_ref, m.mes_carteira)) * 12
                  + EXTRACT(MONTH FROM age(p.mes_ref, m.mes_carteira)))::int
            FROM params p CROSS JOIN mes_carteira m
        ),
        'gerado_em',                  to_char(now(), 'YYYY-MM-DD HH24:MI:SS'),
        'ultimo_mes_dre_disponivel',  (SELECT MAX(mes_debito) FROM marts.resultado),
        'aviso_meses_futuros',        'Meses posteriores ao mes_ref em marts.resultado e marts.consumo são lançamentos futuros pré-agendados (despesas fixas), não realizados. Nunca incluir no diagnóstico.'
    ) AS j
),

b_dre AS (
    SELECT COALESCE(json_agg(t ORDER BY t.mes_debito), '[]'::json) AS j
    FROM (
        SELECT *
        FROM marts.resultado
        WHERE mes_debito <= (SELECT mes_ref FROM params)
          AND mes_debito >  (SELECT mes_ref FROM params) - interval '13 months'
    ) AS t
),

b_consumo_dia AS (
    SELECT COALESCE(json_agg(t ORDER BY t.data), '[]'::json) AS j
    FROM (
        SELECT *
        FROM marts.consumo
        WHERE mes = (SELECT mes_ref FROM params)
    ) AS t
),

b_patrimonio AS (
    SELECT COALESCE(json_agg(t ORDER BY t.mes_base), '[]'::json) AS j
    FROM (
        SELECT *
        FROM marts.patrimonio
        WHERE mes_base <= (SELECT mes_ref FROM params)
          AND mes_base >  (SELECT mes_ref FROM params) - interval '13 months'
    ) AS t
),

b_patrimonio_mom AS (
    SELECT COALESCE(json_agg(t ORDER BY t.mes_base), '[]'::json) AS j
    FROM (
        SELECT mes_base,
               ROUND(total_patrimonio_bruto   * 100, 2) AS pct_patrimonio_bruto,
               ROUND(total_patrimonio_liquido * 100, 2) AS pct_patrimonio_liquido,
               ROUND(patrimonio_liquido_lucas * 100, 2) AS pct_lucas,
               ROUND(patrimonio_liquido_jessica * 100, 2) AS pct_jessica
        FROM marts.patrimonio_mom
        WHERE mes_base <= (SELECT mes_ref FROM params)
          AND mes_base >  (SELECT mes_ref FROM params) - interval '13 months'
    ) AS t
),

b_riqueza AS (
    SELECT COALESCE(json_agg(t ORDER BY t.mes_base), '[]'::json) AS j
    FROM (
        SELECT *
        FROM marts.riqueza
        WHERE mes_base <= (SELECT mes_ref FROM params)
          AND mes_base >  (SELECT mes_ref FROM params) - interval '13 months'
    ) AS t
),

b_dividendos AS (
    SELECT COALESCE(json_agg(t ORDER BY t.mes_base, t.pessoa), '[]'::json) AS j
    FROM (
        SELECT *
        FROM marts.dividendos
        WHERE mes_base <= (SELECT mes_ref FROM params)
          AND mes_base >  (SELECT mes_ref FROM params) - interval '13 months'
    ) AS t
),

b_carteira_camada AS (
    SELECT COALESCE(json_agg(t ORDER BY t.pessoa, t.camada), '[]'::json) AS j
    FROM (
        SELECT c.pessoa,
               c.camada,
               COUNT(*)                        AS qtd_ativos,
               SUM(c.vlr_atualizado_brl)       AS valor,
               ROUND(100.0 * SUM(c.vlr_atualizado_brl) / NULLIF(p.total, 0), 1) AS pct_da_carteira
        FROM carteira_ref AS c
        JOIN total_por_pessoa AS p USING (pessoa)
        GROUP BY c.pessoa, c.camada, p.total
    ) AS t
),

b_carteira_instituicao AS (
    SELECT COALESCE(json_agg(t ORDER BY t.pessoa, t.valor DESC), '[]'::json) AS j
    FROM (
        SELECT c.pessoa,
               c.instituicao,
               SUM(c.vlr_atualizado_brl) AS valor,
               ROUND(100.0 * SUM(c.vlr_atualizado_brl) / NULLIF(p.total, 0), 1) AS pct_da_carteira
        FROM carteira_ref AS c
        JOIN total_por_pessoa AS p USING (pessoa)
        GROUP BY c.pessoa, c.instituicao, p.total
    ) AS t
),

b_carteira_tipo AS (
    SELECT COALESCE(json_agg(t ORDER BY t.pessoa, t.valor DESC), '[]'::json) AS j
    FROM (
        SELECT c.pessoa,
               c.classe_ativo,
               c.tipo_ativo,
               SUM(c.vlr_atualizado_brl) AS valor,
               ROUND(100.0 * SUM(c.vlr_atualizado_brl) / NULLIF(p.total, 0), 1) AS pct_da_carteira
        FROM carteira_ref AS c
        JOIN total_por_pessoa AS p USING (pessoa)
        GROUP BY c.pessoa, c.classe_ativo, c.tipo_ativo, p.total
    ) AS t
),

b_posicoes AS (
    SELECT COALESCE(json_agg(t ORDER BY t.pessoa, t.vlr_atualizado_brl DESC), '[]'::json) AS j
    FROM (
        SELECT pessoa, camada, classe_ativo, tipo_ativo,
               ativo, instituicao, emissor, conglomerado, indexador,
               data_vencimento, vencimento_em_dias, vlr_atualizado_brl, moeda_ativo
        FROM carteira_ref
    ) AS t
),

b_nao_classificados AS (
    SELECT COALESCE(json_agg(t ORDER BY t.pessoa, t.vlr_atualizado_brl DESC), '[]'::json) AS j
    FROM (
        SELECT pessoa, ativo, classe_ativo, tipo_ativo,
               instituicao, vlr_atualizado_brl
        FROM carteira_ref
        WHERE camada = 'NAO CLASSIFICADO'
    ) AS t
),

b_vencimentos AS (
    SELECT COALESCE(json_agg(t ORDER BY t.data_vencimento), '[]'::json) AS j
    FROM (
        SELECT pessoa, ativo, emissor, indexador, instituicao,
               data_vencimento, vencimento_em_dias, vlr_atualizado_brl, camada
        FROM carteira_ref
        WHERE data_vencimento IS NOT NULL
          AND vencimento_em_dias <= 365
    ) AS t
),

b_fgc AS (
    SELECT COALESCE(json_agg(t ORDER BY t.pessoa, t.vlr_liberado), '[]'::json) AS j
    FROM (
        SELECT 'lucas'   AS pessoa, * FROM marts.risco_fgc_lucas
        UNION ALL
        SELECT 'jessica' AS pessoa, * FROM marts.risco_fgc_jessica
        UNION ALL
        SELECT 'deusa'   AS pessoa, * FROM marts.risco_fgc_deusa
    ) AS t
),

b_moeda AS (
    SELECT COALESCE(json_agg(t ORDER BY t.pessoa, t.moeda_ativo), '[]'::json) AS j
    FROM (
        SELECT c.pessoa,
               c.moeda_ativo,
               SUM(c.vlr_atualizado_brl) AS valor,
               ROUND(100.0 * SUM(c.vlr_atualizado_brl) / NULLIF(p.total, 0), 1) AS pct_da_carteira
        FROM carteira_ref AS c
        JOIN total_por_pessoa AS p USING (pessoa)
        GROUP BY c.pessoa, c.moeda_ativo, p.total
    ) AS t
),

b_luz AS (
    SELECT COALESCE(json_agg(t ORDER BY t.mes), '[]'::json) AS j
    FROM (
        SELECT *
        FROM marts.luz
        WHERE mes <= (SELECT mes_ref FROM params)
          AND mes >  (SELECT mes_ref FROM params) - interval '13 months'
    ) AS t
)

SELECT json_build_object(
    'meta',                  (SELECT j FROM b_meta),
    'dre_mensal',            (SELECT j FROM b_dre),
    'consumo_diario',        (SELECT j FROM b_consumo_dia),
    'patrimonio',            (SELECT j FROM b_patrimonio),
    'patrimonio_mom_pct',    (SELECT j FROM b_patrimonio_mom),
    'riqueza',               (SELECT j FROM b_riqueza),
    'dividendos',            (SELECT j FROM b_dividendos),
    'carteira_camada',       (SELECT j FROM b_carteira_camada),
    'carteira_instituicao',  (SELECT j FROM b_carteira_instituicao),
    'carteira_tipo',         (SELECT j FROM b_carteira_tipo),
    'carteira_moeda',        (SELECT j FROM b_moeda),
    'posicoes',              (SELECT j FROM b_posicoes),
    'nao_classificados',     (SELECT j FROM b_nao_classificados),
    'vencimentos_12m',       (SELECT j FROM b_vencimentos),
    'fgc',                   (SELECT j FROM b_fgc),
    'luz',                   (SELECT j FROM b_luz)
)::text;
