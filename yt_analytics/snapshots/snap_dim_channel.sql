{% snapshot snap_dim_channel %}

{{
    config(
        target_schema='snapshots',
        unique_key='channel_id',
        strategy='check',
        check_cols=['channel_name'],
        invalidate_hard_deletes=True
    )
}}

select
    `Channel ID`                                        as channel_id,
    `Channel Name`                                      as channel_name,
    parse_date('%d/%m/%Y', `Channel published Date`)    as channel_published_date
from {{ source('raw', 'dim_channel') }}
where `Channel ID` is not null

{% endsnapshot %}