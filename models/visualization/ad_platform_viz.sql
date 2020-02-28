select
date,
site,
platform,
ifnull(campaign,'') Campaign,
ifnull(group_set,'') ADGroup,
ifnull(ad, '') AdName,
ifnull(objective, '') Objective,
ifnull(placement, '') Placement,
ifnull(spend, 0) Spend,
ifnull(impressions, 0) Impressions,
ifnull(clicks, 0) TotalClicks,
ifnull(link_clicks, 0) LinkClicks,
ifnull(conversions, 0) Conversions,
round(cpm,2) CPM,
round(cpc,2) CPC,
round(cpc_link,2) CPC_Link,
round(cpconversion,2) CPResult

FROM {{ ref('adplatform_cost_calculations') }}