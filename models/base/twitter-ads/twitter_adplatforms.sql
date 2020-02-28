select
account,
platform,
date,
campaign,
group_set,
ad,
objective,
placement,
spend,
impressions,
clicks,
link_clicks,
conversions
from(
    select
    account,
    'Twitter Ads' as platform,
    date,
    campaign,
    group_set,
    ad,
    campaign_type as objective,
    placement,
    cost spend,
    impressions,
    clicks,
    link_clicks,
    conversions,
    time_of_entry,
    first_value(time_of_entry) over (partition by date order by time_of_entry desc) lv
    from `{{ target.project }}.agency_data_pipeline.twitter_ads` 
    )
where time_of_entry = lv