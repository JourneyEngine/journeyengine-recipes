SELECT
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
FROM

{{ ref('fb_adplatforms') }}

UNION ALL

SELECT
account,
platform,
date,
campaign,
group_set,
ad,
cast(objective as string),
placement,
spend,
impressions,
clicks,
link_clicks,
conversions
FROM

{{ ref('adwords_adplatforms') }}

UNION ALL

SELECT
account,
platform,
CAST(date AS TIMESTAMP),
campaign,
group_set,
ad,
CAST(objective AS STRING),
placement,
spend,
impressions,
clicks,
link_clicks,
conversions
FROM

{{ ref('linkedin_adplatforms') }}

UNION ALL

SELECT
account,
platform,
cast(date as timestamp),
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
FROM 

{{ ref('twitter_adplatforms') }}