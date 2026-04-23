{{ config(
    materialized='incremental',
    unique_key='metric_id',
    incremental_strategy='merge'
) }}

with fact as (
    select * from {{ ref('stg_fact_daily_video_metrics') }}
    {% if is_incremental() %}
        where metrics_date > (select max(metrics_date) from {{ this }})
    {% endif %}
),

videos as (
    select * from {{ ref('stg_dim_video') }}
),

channels as (
    select * from {{ ref('stg_dim_channel') }}
),

categories as (
    select * from {{ ref('stg_dim_category') }}
)

select
    -- surrogate key
    f.metric_id,

    -- fact grain
    f.video_id,
    f.metrics_date,
    f.views,
    f.watch_time_minutes,
    f.subscriber_net_change,
    f.comments,
    f.likes,
    f.shares,
    f.thumbnail_impressions,
    f.ctr_pct,

    -- video attributes
    v.channel_id,
    v.video_published_date,
    v.video_title,
    v.video_length_seconds,
    v.video_type,
    v.category_id,

    -- channel attributes
    c.channel_name,
    c.channel_published_date,

    -- category
    cat.category_name,

    -- derived date parts
    date_trunc('month', f.metrics_date) as metrics_month

from fact f
left join videos     v   on f.video_id    = v.video_id
left join channels   c   on v.channel_id  = c.channel_id
left join categories cat on v.category_id = cat.category_id