{{ config(materialized='view') }}

with source as (
    select * from {{ ref('dim_category') }}
),

renamed as (
    select
        "Category ID"      as category_id,
        "YouTube Category" as category_name
    from source
    where "Category ID" is not null
)

select * from renamed