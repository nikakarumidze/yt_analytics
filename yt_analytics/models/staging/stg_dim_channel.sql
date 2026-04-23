{{ config(materialized='view') }}
with source as (
    select * from {{ source('raw', 'dim_channel') }}
),

renamed as (
    select
        "Channel ID"                                        as channel_id,
        "Channel Name"                                      as channel_name,
        strptime("Channel published Date", '%d/%m/%Y')::date as channel_published_date
    from source
    where "Channel ID" is not null
)

select * from renamed