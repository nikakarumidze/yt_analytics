{{ config(materialized='table') }}

with source as (
    select * from {{ ref('fact_daily_video_metrics') }}
),

cleaned as (
    select
        "Video ID"                                                              as video_id,
        strptime("Metrics Date", '%d/%m/%Y')::date                              as metrics_date,
        cast(replace("Views", ',', '') as integer)                              as views,
        cast("Watch Time Minutes" as double)                                    as watch_time_minutes,
        cast("Subscriber Net Change" as integer)                                as subscriber_net_change,
        cast("Comments" as integer)                                             as comments,
        greatest(cast("Likes" as integer), 0)                                   as likes,
        cast("Shares" as integer)                                               as shares,
        cast("Thumbnail Impressions" as integer)                                as thumbnail_impressions,
        cast("Video level thumbnail impressions CTR (percentage)" as double)    as ctr_pct
    from source
    where "Video ID" is not null
      and "Metrics Date" is not null
      and cast(replace("Views", ',', '') as integer) >= 0
      and cast("Watch Time Minutes" as double) >= 0
      and cast("Comments" as integer) >= 0
      and cast("Shares" as integer) >= 0
      and cast("Thumbnail Impressions" as integer) >= 0
),

deduped as (
    select *,
        row_number() over (
            partition by video_id, cast(metrics_date as varchar)
            order by views desc
        ) as rn
    from cleaned
)

select
    video_id || '_' || cast(metrics_date as varchar)    as metric_id,
    video_id,
    metrics_date,
    views,
    watch_time_minutes,
    subscriber_net_change,
    comments,
    likes,
    shares,
    thumbnail_impressions,
    ctr_pct
from deduped where rn = 1