{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

{#-
  Recorte por pessoa de int_carteira_agregada, que já resolve o pivot dinâmico
  por instituição. Instituição nova entra como coluna aqui sem edição manual e
  entra nos totais — antes o CTE `final` listava as instituições uma a uma e
  descartava as que não estivessem na lista.
-#}

SELECT
    mes_base,
    mes_final,
    pessoa,
    total_geral,
    total_disponibilidades,
    total_investido,
    bradesco,
    nubank,
    autocustodia,
    daycoval,
    avenue,
    wise,
    vlr_liquido_usd,
    CURRENT_TIMESTAMP AS model_updated_at
FROM {{ ref('int_carteira_agregada') }}
WHERE pessoa = 'lucas'
ORDER BY mes_final
