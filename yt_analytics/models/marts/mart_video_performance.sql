select
    video_id,
    video_title,
    channel_name,
    category_name,
    video_type,
    video_published_date,
    video_length_seconds,
    sum(views)                                                      as total_views,
    sum(watch_time_minutes)                                         as total_watch_time_minutes,
    sum(likes)                                                      as total_likes,
    sum(shares)                                                     as total_shares,
    sum(comments)                                                   as total_comments,
    sum(watch_time_minutes) / nullif(sum(views), 0)                 as avg_watch_time_per_view_minutes,
    sum(likes) / nullif(sum(views), 0)                              as like_rate,
    avg(ctr_pct)                                                    as avg_ctr,
    rank() over (
        partition by channel_name
        order by sum(views) desc
    )                                                               as rank_by_views_in_channel

from {{ ref('int_video_metrics_joined') }}
group by 1, 2, 3, 4, 5, 6, 7