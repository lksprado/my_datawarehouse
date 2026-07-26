{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

{#-
  Lista de instituições obtida em tempo de compilação a partir de int_carteira.
  O `default` garante colunas mesmo numa build do zero, quando int_carteira
  ainda não foi materializada e a consulta retornaria vazio.

  ATENÇÃO: o pivot é dinâmico, mas o CTE `final` abaixo lista as instituições uma
  a uma. Instituição nova vira coluna aqui e é descartada logo em seguida — não
  chega ao mart nem entra em `total_investido`. Ao abrir conta em uma instituição
  nova, editar o CTE `final` à mão.
-#}
{%- set instituicoes = dbt_utils.get_column_values(
    ref('int_carteira'),
    'instituicao',
    order_by = 'instituicao',
    default = ['SOFISA', 'ITAU', 'NUBANK', 'AVENUE', 'DESCONHECIDO']
) -%}

WITH
agregrada AS (
    SELECT
        mes_final,
      {{ dbt_utils.pivot(
          'instituicao',
          instituicoes,
          agg = 'sum',
          then_value = 'vlr_atualizado_brl',
          quote_identifiers = False
      ) }}
    FROM {{ ref('int_carteira') }}
    WHERE pessoa = 'jessica'
    GROUP BY mes_final
    ORDER BY mes_final
),

final AS (
    SELECT
        mes_final,
        sofisa,
        itau,
        nubank,
        avenue,
        (
            sofisa
            + itau
            + nubank
            + avenue
        ) AS total_investido
    FROM agregrada
)

SELECT * FROM final
ORDER BY mes_final
