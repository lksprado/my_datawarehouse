{{
  config(
    materialized = 'table',
    tags = ['financas', 'marts'],
  )
}}

WITH var_acum AS (
    SELECT
        mes_base,

        EXP(
            COALESCE(
                SUM(LN(1 + total_patrimonio_liquido)) OVER (
                    ORDER BY mes_base
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
            )
        )::NUMERIC(18,3) AS total_patrimonio_liquido_acum,

        EXP(
            COALESCE(
                SUM(LN(1 + patrimonio_liquido_lucas)) OVER (
                    ORDER BY mes_base
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
            )
        )::NUMERIC(18,3) AS patrimonio_liquido_lucas_acum,

        EXP(
            COALESCE(
                SUM(LN(1 + patrimonio_liquido_jessica)) OVER (
                    ORDER BY mes_base
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
            )
        )::NUMERIC(18,3) AS patrimonio_liquido_jessica_acum

    FROM {{ ref('patrimonio_mom') }}
    WHERE mes_base >= DATE '2023-11-01'
),
indexadores AS (
    SELECT
        mes_base,
        minha_inflacao_acum,
        ipca_acum,
        igpm_acum,
        selic_acum,
        cdi_acum
    FROM {{ ref('int_indexadores') }}
),
final AS (
    SELECT 
    t1.*,
    t2.minha_inflacao_acum,
    t2.ipca_acum,
    t2.igpm_acum,
    t2.selic_acum,
    t2.cdi_acum,
    CASE WHEN total_patrimonio_liquido_acum    > minha_inflacao_acum THEN 'RICOS' ELSE 'POBRES' END AS comparativo_minha_inflacao_total_liquido,
    CASE WHEN patrimonio_liquido_lucas_acum    > minha_inflacao_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_minha_inflacao_lucas,
    CASE WHEN patrimonio_liquido_jessica_acum  > minha_inflacao_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_minha_inflacao_jessica,

    CASE WHEN total_patrimonio_liquido_acum    > ipca_acum THEN 'RICOS' ELSE 'POBRES' END AS comparativo_ipca_total_liquido,
    CASE WHEN patrimonio_liquido_lucas_acum    > ipca_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_ipca_lucas,
    CASE WHEN patrimonio_liquido_jessica_acum  > ipca_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_ipca_jessica,

    CASE WHEN total_patrimonio_liquido_acum    > igpm_acum THEN 'RICOS' ELSE 'POBRES' END AS comparativo_igpm_total_liquido,
    CASE WHEN patrimonio_liquido_lucas_acum    > igpm_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_igpm_lucas,
    CASE WHEN patrimonio_liquido_jessica_acum  > igpm_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_igpm_jessica,

    CASE WHEN total_patrimonio_liquido_acum    > selic_acum THEN 'RICOS' ELSE 'POBRES' END AS comparativo_selic_total_liquido,
    CASE WHEN patrimonio_liquido_lucas_acum    > selic_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_selic_lucas,
    CASE WHEN patrimonio_liquido_jessica_acum  > selic_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_selic_jessica,

    CASE WHEN total_patrimonio_liquido_acum    > cdi_acum THEN 'RICOS' ELSE 'POBRES' END AS comparativo_cdi_total_liquido,
    CASE WHEN patrimonio_liquido_lucas_acum    > cdi_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_cdi_lucas,
    CASE WHEN patrimonio_liquido_jessica_acum  > cdi_acum THEN 'RICO' ELSE 'POBRE' END AS comparativo_cdi_jessica
    FROM var_acum t1
        INNER JOIN indexadores t2
        ON t1.mes_base = t2.mes_base
)
SELECT * FROM final ORDER BY mes_base
