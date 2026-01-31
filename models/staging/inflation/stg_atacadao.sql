{{
  config(
    materialized = 'table',
    tags = ['inflacao', 'staging'],
  )
}}

with
source_1 as (
    select * from {{ref('stg_atacadao_historico') }}
),
source_2 as (
    select * from {{ref('stg_atacadao_novo') }}
),
unioned as (
    select * from source_1 
    union all
    select * from source_2 
),
renamed as (
  select
    *,
    -- pega só as letras: "300g" -> "g", "500ml" -> "ml", "1,5l" -> "l"
    regexp_replace(lower(product_unity), '[0-9.,\s]', '', 'g') as unit_raw,

    case
      when regexp_replace(lower(product_unity), '[0-9.,\s]', '', 'g') in ('kg','quilo','quilos') then 'kg'
      when regexp_replace(lower(product_unity), '[0-9.,\s]', '', 'g') in ('g','grama','gramas') then 'g'
      when regexp_replace(lower(product_unity), '[0-9.,\s]', '', 'g') in ('ml') then 'ml'
      when regexp_replace(lower(product_unity), '[0-9.,\s]', '', 'g') in ('l','litro','litros') then 'l'
      when regexp_replace(lower(product_unity), '[0-9.,\s]', '', 'g') in ('un','uni','unid','unidade','unidades', 'rolos', 'dúzias', 'folhas') then 'un'
      else null
    end as unit_normalized
  from unioned
  
)
select *,
case
    when quantity_type = 'volume' and unit_normalized = 'ml'
        then quantity_value_raw / 1000
    when quantity_type = 'volume' and unit_normalized = 'l'
        then quantity_value_raw
    when quantity_type = 'weight' and unit_normalized = 'g'
        then quantity_value_raw / 1000
    when quantity_type = 'weight' and unit_normalized = 'kg'
        then quantity_value_raw
    when quantity_type = 'unit'
        then quantity_value_raw
end
 as quantity_value_normalized


from renamed where unit_normalized is not null order by created_at