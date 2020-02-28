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
    'LinkedIn Ads' as platform,
    date,
    campaign,
    group_set,
    ad,
    null as objective,
    campaign_type placement,
    cost spend,
    impressions,
    clicks,
    link_clicks,
    conversions,
    time_of_entry,
    first_value(time_of_entry) over (partition by date order by time_of_entry desc) lv
    from `journeyengine-recipes.agency_data_pipeline.linkedin_ads`
  )
  where time_of_entry = lv