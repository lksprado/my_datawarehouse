{{
  config(
    materialized = 'ephemeral',
    tags = ['investimentos', 'staging'],
  )
}}

with
source as (
    select * from {{ source('raw', 'avenue_legacy')}}
),
renamed as (
    select 
    to_date(period_start, 'yyyy-MM-dd') as period_start,
    to_date(period_end, 'yyyy-MM-dd') as period_end,
    {{ clean_string("asset_class", "upper") }} as asset_class,
    {{ clean_string("description", "upper") }} as description,
    symbol_cusip,
    market_value::numeric(18,2) as market_value,
    person AS pessoa   
    from source
),
final as (
    select 
    case 
        when period_end - period_start > 31 then date_trunc('month', period_end)
        else period_start
    end::date as period_start,
    period_end,
    asset_class,
    CASE 
        WHEN symbol_cusip IN('91282CLH2', '7009170', '7381496')
        THEN 'US TREASURY'
        ELSE description
    END AS description,
    CASE 
        WHEN symbol_cusip = '91282CLH2'
        THEN '7009170'
        ELSE symbol_cusip
    END AS symbol_cusip,
    market_value,
    pessoa
    from renamed
)
select * from final