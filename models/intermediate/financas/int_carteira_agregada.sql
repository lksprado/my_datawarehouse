{{
  config(
    materialized = 'table',
    tags = ['financas', 'intermediate'],
  )
}}

{#-
  Carteira agregada por mês x pessoa, com uma coluna por instituição.

  `pessoa` fica no GROUP BY (linha), não no pivot: dbt_utils.pivot gira UMA
  coluna só, e cruzar pessoa x instituição em colunas exigiria passar uma
  expressão concatenada — daria ~30 colunas, a maioria zerada, e o conjunto
  mudaria a cada conta aberta ou fechada. Mantendo pessoa como grão da linha,
  este modelo substitui os três carteira_*_agregada, que viram um filtro por
  pessoa sobre ele.

  Materializado como table (foge do default view do intermediate) porque os três
  marts leem o resultado inteiro; como view, o union + pivot seria recalculado
  em cada um deles. Sem model_updated_at aqui — é convenção dos marts, e os três
  que leem este modelo com SELECT * carimbam o próprio.

  Totais: `total_investido` conta só as posições de int_carteira e
  `total_disponibilidades` só as de int_carteira_extra, preservando a série
  histórica dos marts antigos (que liam apenas int_carteira). O corte é por
  modelo de origem, não por classe_ativo — int_carteira já traz algumas linhas
  'DISPONIBILIDADE' vindas de int_renda_variavel (tipo_ativo CONTA CORRENTE) e
  elas sempre entraram no total dos marts. As colunas por instituição somam
  TODAS as posições, então vale `soma das colunas = total_geral`.

  `vlr_liquido_usd` (saldo Avenue em moeda original) vem de int_carteira_usd e é
  juntado DEPOIS da agregação, de propósito: aquele modelo já está no grão
  mês x pessoa, então o join é 1:1 com o resultado e não com `posicoes`. Dentro
  do GROUP BY o valor era replicado em cada linha de posição e somado N vezes —
  saía inflado pela contagem de posições da pessoa no mês.

  A lista de instituições é lida em tempo de compilação das duas fontes e unida;
  o `default` cobre a build do zero, quando as tabelas ainda não existem e a
  consulta voltaria vazia. O guard em `execute` é necessário porque
  get_column_values devolve Undefined — e não o default — na fase de parse
  (dbt_utils 1.4.1 + Jinja 3.1), o que estouraria no `+`/`unique` abaixo; os
  ref() ficam fora do guard para o DAG continuar enxergando as dependências.
-#}
{%- set rel_carteira = ref('int_carteira') -%}
{%- set rel_extra = ref('int_carteira_extra') -%}
{%- set default_carteira = ['AVENUE', 'BANCO DO BRASIL', 'BRADESCO', 'DAYCOVAL', 'DESCONHECIDO', 'ITAU', 'NUBANK', 'SOFISA'] -%}
{%- set default_extra = ['AUTOCUSTODIA', 'BANCO DO BRASIL', 'BRADESCO', 'NUBANK', 'WISE'] -%}

{%- if execute -%}
    {%- set inst_carteira = dbt_utils.get_column_values(rel_carteira, 'instituicao', default = default_carteira) -%}
    {%- set inst_extra = dbt_utils.get_column_values(rel_extra, 'instituicao', default = default_extra) -%}
{%- else -%}
    {%- set inst_carteira = default_carteira -%}
    {%- set inst_extra = default_extra -%}
{%- endif -%}

{%- set instituicoes = (inst_carteira + inst_extra) | reject('none') | unique | sort -%}

WITH
posicoes AS (
    SELECT
        mes_base,
        mes_final,
        pessoa,
        instituicao,
        vlr_atualizado_brl,
        'carteira' AS origem
    FROM {{ rel_carteira }}

    UNION ALL

    SELECT
        mes_base,
        mes_final,
        pessoa,
        instituicao,
        vlr_atualizado_brl,
        'extra' AS origem
    FROM {{ rel_extra }}
),

final AS (
    SELECT
        p.mes_base,
        p.mes_final,
        p.pessoa,
        SUM(p.vlr_atualizado_brl)                                                 AS total_geral,
        COALESCE(SUM(p.vlr_atualizado_brl) FILTER (WHERE origem = 'extra'), 0)    AS total_disponibilidades,
        COALESCE(SUM(p.vlr_atualizado_brl) FILTER (WHERE origem = 'carteira'), 0) AS total_investido,
        {{ dbt_utils.pivot(
            'instituicao',
            instituicoes,
            agg = 'sum',
            then_value = 'vlr_atualizado_brl',
            quote_identifiers = False
        ) }}
    FROM posicoes p
    GROUP BY p.mes_base, p.mes_final, p.pessoa
)

SELECT
    f.*,
    COALESCE(u.vlr_liquido_usd, 0) AS vlr_liquido_usd
FROM final f
LEFT JOIN {{ ref('int_carteira_usd') }} u
    ON u.period_start = f.mes_base
    AND u.pessoa = f.pessoa
ORDER BY f.mes_final, f.pessoa
