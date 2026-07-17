{% test is_equal(model, column_name, compare_model, compare_column) %}

with model_1 as (
    select
        {{ column_name }} as valor
    from {{ model }}
),

model_2 as (
    select
        {{ compare_column }} as valor
    from {{ compare_model }}
)

select valor
from model_1

except

select valor
from model_2

union all

select valor
from model_2

except

select valor
from model_1

{% endtest %}