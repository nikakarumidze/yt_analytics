select
    channel_id,
    channel_name,
    channel_published_date,
    count(distinct video_id)                                        as video_count,
    sum(views)                                                      as total_views,
    sum(watch_time_minutes)                                         as total_watch_time_minutes,
    sum(likes)                                                      as total_likes,
    sum(subscriber_net_change)                                      as net_subscriber_gain,
    sum(shares)                                                     as total_shares,
    sum(comments)                                                   as total_comments,
    sum(thumbnail_impressions)                                      as total_impressions,
    sum(watch_time_minutes) / nullif(sum(views), 0)                 as avg_watch_time_per_view_minutes,
    sum(likes) / nullif(sum(views), 0)                              as like_rate,
    avg(ctr_pct)                                                    as avg_ctr

from {{ ref('int_video_metrics_joined') }}
group by 1, 2, 3