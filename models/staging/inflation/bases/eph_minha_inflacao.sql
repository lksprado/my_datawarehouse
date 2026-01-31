{{
  config(
    materialized = 'ephemeral',
    tags = ['inflacao', 'staging'],
  )
}}

with 
source as (
    select * from {{ source('raw', 'minha_inflacao') }}
),
renamed as (
    select 
        regexp_replace(public.unaccent(lower("Produto")), '[^a-z0-9]', '', 'g') clean_product_name,
        "Produto" as product_name,
        "Unidade" as product_unity,
        "Qtd" as quantity,
        "Preço" as price,
        "Mês Atual" as current_month,
        "Peso" as weight,
        "Mês passado" as past_month,
        "Var" as price_variation,
        "Var Peso" as weight_variation,
        "Categoria" as category,
        "Mes" as month,
        "Ano" as year
    from source 
),
units as (
    select 
    distinct
    {{ dbt_utils.generate_surrogate_key(['clean_product_name']) }} as sk_product, 
    replace(lower(public.unaccent(product_name)),' ','_') as product_name,
    replace(trim(lower(product_unity)),'.',',') as product_unity,
    case
        -- PESO
        when lower(product_unity) ~* '(kg|quilo|grama|g\\b)' then 'weight'
        -- VOLUME
        when lower(product_unity) ~* '(litro|l\\b|ml)' then 'volume'
        -- UNIDADE / EMBALAGEM CONTÁVEL
        when lower(product_unity) ~* '(fardo|caixa|pacote|barra|pote|maço|unidade|sabonete)' then 'unit'
        else 'unknown'
    end as quantity_type,
        -- VALOR NUMÉRICO BRUTO
    case
        when lower(product_unity) ~* '^(kg|quilo|litro|l|ml)$' then 1::numeric

        when lower(product_unity) ~* '[0-9]' then
            replace(
                regexp_replace(lower(product_unity), '[^0-9.,]', '', 'g'),
                ',',
                '.'
            )::numeric

        else 1
    end as quantity_value_raw
    from renamed 
),
final as (
    select
    row_number() over (partition by product_name) as rn,
    *,

    -- VALOR PADRONIZADO (g / ml / unidades)
    case
        when lower(product_unity) ~* 'kg' then quantity_value_raw * 1000
        when lower(product_unity) ~* '(grama|g\b)' then quantity_value_raw
        when lower(product_unity) ~* 'litro' then quantity_value_raw * 1000
        when lower(product_unity) ~* 'ml' then quantity_value_raw
        when quantity_type = 'unit' then quantity_value_raw
        else 1
    end as quantity_value_std
     from units
)
select
sk_product,
product_name,
product_unity,
quantity_type,
quantity_value_raw,
quantity_value_std
from final
where rn = 2
