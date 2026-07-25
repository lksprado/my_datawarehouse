{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

{#-
  Lista de instituições obtida em tempo de compilação a partir de int_carteira.
  Ao surgir uma instituição nova, ela vira coluna automaticamente no próximo dbt run.
  O `default` garante colunas mesmo numa build do zero, quando int_carteira
  ainda não foi materializada e a consulta retornaria vazio.
-#}
{%- set instituicoes = dbt_utils.get_column_values(
    ref('int_carteira'),
    'instituicao',
    order_by = 'instituicao',
    default = ['BRADESCO', 'NUBANK', 'BANCO DO BRASIL','AVENUE', 'DESCONHECIDO']
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
    WHERE pessoa = 'deusa'
    GROUP BY mes_final
    ORDER BY mes_final
),

final AS (
    SELECT
        mes_final,
        banco_do_brasil,
        bradesco,
        nubank,
        avenue,
        (
            banco_do_brasil
            + bradesco
            + nubank
            + avenue
        ) AS total_investido
    FROM agregrada
)

SELECT * FROM final
ORDER BY mes_final
