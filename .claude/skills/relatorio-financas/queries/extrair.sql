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

-- --------------------------------------------------------------- portão ---
-- O mês de referência está fechado? Três sinais separam os casos que importam:
--
--   mês fechado      26-28 dias com gasto, lançamento até o último dia do mês
--   mês em andamento para no dia de hoje
--   mês pré-lançado  só as fixas (dia 25), variáveis zeradas
--
-- Serve para o relatório recusar um mês que ainda não terminou de carregar, em
-- vez de sair confiante em cima de meia planilha.

gasto_diario AS (
    SELECT
        mes,
        data,
        total_mercado + total_diversos + total_assinaturas + total_role
        + total_transporte + total_apartamento + total_saude + total_educacao AS total_dia
    FROM marts.consumo
),

cobertura_mes AS (
    SELECT
        g.mes,
        COUNT(*)                                     AS dias_no_mes,
        COUNT(*) FILTER (WHERE g.total_dia <> 0)     AS dias_com_gasto,
        MAX(g.data) FILTER (WHERE g.total_dia <> 0)  AS ultimo_dia_com_gasto,
        (g.mes + interval '1 month' - interval '1 day')::date AS fim_do_mes
    FROM gasto_diario AS g
    GROUP BY g.mes
),

-- Regra do portão, aplicada a todos os meses para também responder qual foi o
-- último que fecharia.
meses_fechados AS (
    SELECT c.mes
    FROM cobertura_mes AS c
    INNER JOIN marts.resultado AS r ON r.mes_debito = c.mes
    WHERE r.total_role + r.total_diversos + r.total_transporte > 0
      AND c.ultimo_dia_com_gasto IS NOT NULL
      AND (c.fim_do_mes - c.ultimo_dia_com_gasto) <= 2
),

prontidao AS (
    SELECT
        COALESCE(d.presente, FALSE)                                AS dre_presente,
        COALESCE(d.com_variaveis, FALSE)                           AS dre_com_variaveis,
        c.dias_no_mes,
        c.dias_com_gasto,
        c.ultimo_dia_com_gasto,
        (c.fim_do_mes - c.ultimo_dia_com_gasto)                    AS dias_sem_lancamento_no_fim,
        (SELECT (EXTRACT(YEAR FROM age(p.mes_ref, m.mes_carteira)) * 12
               + EXTRACT(MONTH FROM age(p.mes_ref, m.mes_carteira)))::int
         FROM params p CROSS JOIN mes_carteira m)                  AS carteira_defasagem_meses,
        (SELECT MAX(mes) FROM meses_fechados)                      AS ultimo_mes_fechado
    FROM params AS pa
    LEFT JOIN cobertura_mes AS c ON c.mes = pa.mes_ref
    LEFT JOIN LATERAL (
        SELECT
            COUNT(*) > 0 AS presente,
            COALESCE(SUM(r.total_role + r.total_diversos + r.total_transporte), 0) > 0
                AS com_variaveis
        FROM marts.resultado AS r
        WHERE r.mes_debito = pa.mes_ref
    ) AS d ON TRUE
),

pendencias AS (
    SELECT COALESCE(json_agg(t.frase), '[]'::json) AS j
    FROM (
        SELECT 'Não há lançamento em marts.resultado para o mês de referência.' AS frase
        FROM prontidao WHERE NOT dre_presente
        UNION ALL
        SELECT 'O mês só tem as despesas fixas recorrentes — role, diversos e transporte estão zerados. É um mês pré-lançado, não realizado.'
        FROM prontidao WHERE dre_presente AND NOT dre_com_variaveis
        UNION ALL
        SELECT 'Último gasto lançado em ' || to_char(ultimo_dia_com_gasto, 'DD/MM')
               || ', a ' || dias_sem_lancamento_no_fim || ' dias do fim do mês: o mês ainda não terminou de carregar.'
        FROM prontidao WHERE dias_sem_lancamento_no_fim > 2
        UNION ALL
        SELECT 'Nenhum dia com gasto lançado no mês.'
        FROM prontidao WHERE ultimo_dia_com_gasto IS NULL
        UNION ALL
        SELECT 'A carteira está ' || carteira_defasagem_meses
               || ' meses atrás do mês de referência — defasagem grande demais para comparar patrimônio e orçamento.'
        FROM prontidao WHERE carteira_defasagem_meses > 2
    ) AS t
),

-- ---------------------------------------------------------------- blocos ---

b_meta AS (
    SELECT json_build_object(
        'mes_ref',                    (SELECT mes_ref FROM params),
        'mes_carteira',               (SELECT mes_carteira FROM mes_carteira),
        'defasagem_carteira_meses',   (SELECT carteira_defasagem_meses FROM prontidao),
        'gerado_em',                  to_char(now(), 'YYYY-MM-DD HH24:MI:SS'),
        'hoje',                       current_date,
        'ultimo_mes_dre_disponivel',  (SELECT MAX(mes_debito) FROM marts.resultado),
        'ultimo_mes_fechado',         (SELECT ultimo_mes_fechado FROM prontidao),
        'motivos_especiais',          (
            SELECT NULLIF(r.motivo, 'NORMAL')
            FROM marts.resultado r CROSS JOIN params p
            WHERE r.mes_debito = p.mes_ref
        ),
        'prontidao',                  (
            SELECT json_build_object(
                'pronto', (dre_presente
                       AND dre_com_variaveis
                       AND COALESCE(dias_sem_lancamento_no_fim, 99) <= 2
                       AND carteira_defasagem_meses <= 2),
                'dre_presente',               dre_presente,
                'dre_com_variaveis',          dre_com_variaveis,
                'dias_no_mes',                dias_no_mes,
                'dias_com_gasto',             dias_com_gasto,
                'ultimo_dia_com_gasto',       ultimo_dia_com_gasto,
                'dias_sem_lancamento_no_fim', dias_sem_lancamento_no_fim,
                'carteira_defasagem_meses',   carteira_defasagem_meses,
                'ultimo_mes_fechado',         ultimo_mes_fechado,
                'pendencias',                 (SELECT j FROM pendencias)
            )
            FROM prontidao
        ),
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
