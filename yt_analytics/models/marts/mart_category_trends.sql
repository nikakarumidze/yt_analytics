select
    category_name,
    metrics_month,
    video_type,
    count(distinct video_id)                                        as active_videos,
    sum(views)                                                      as total_views,
    sum(watch_time_minutes)                                         as total_watch_time_minutes,
    sum(likes)                                                      as total_likes,
    sum(watch_time_minutes) / nullif(sum(views), 0)                 as avg_watch_time_per_view_minutes,
    sum(likes) / nullif(sum(views), 0)                              as like_rate,
    avg(ctr_pct)                                                    as avg_ctr

from {{ ref('int_video_metrics_joined') }}
group by 1, 2, 3