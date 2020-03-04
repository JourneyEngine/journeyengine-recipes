SELECT 
date, 
account, 
channel,
platform,
url,
cost,
impressions,
clicks,
conversions
FROM 
{{ ref('adwords_stats') }}

UNION ALL  

SELECT 
date, 
account, 
channel,
platform,
url,
cost,
impressions,
clicks,
conversions
FROM 
{{ref('fb_ads_stats')}}

UNION ALL

SELECT 
date, 
account, 
channel,
platform,
url,
cost,
impressions,
clicks,
conversions
FROM 
{{ref('gsc_stats')}}

UNION ALL

SELECT 
date, 
account, 
'Paid' as channel,
platform,
null as url,
spend cost,
impressions,
clicks,
conversions
FROM 
{{ref('linkedin_adplatforms')}}