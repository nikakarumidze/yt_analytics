{{ config(materialized='view') }}

with source as (
    select * from {{ ref('dim_video') }}
),

deduped as (
    select *,
        row_number() over (
            partition by "Video ID"
            order by "Video Published Date"
        ) as rn
    from source
    where "Video ID" is not null
),

renamed as (
    select
        "Video ID"                                              as video_id,
        "Channel ID"                                            as channel_id,
        strptime("Video Published Date", '%d/%m/%Y')::date      as video_published_date,
        "Video Title"                                           as video_title,
        cast("Video Length (Seconds)" as integer)               as video_length_seconds,
        "Video data Shorts"                                     as video_type,
        "Video data YouTube category"                           as category_id
    from deduped
    where rn = 1
)

select * from renamed
