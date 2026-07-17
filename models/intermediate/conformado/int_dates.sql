{{ config(
    tags=["datas"]
) }}

{{ dbt_date.get_date_dimension("2020-01-01", "2050-12-31") }}