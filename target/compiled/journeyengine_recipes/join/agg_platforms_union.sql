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
`journeyengine-recipes`.`agency_data_pipeline`.`adwords_stats`

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
`journeyengine-recipes`.`agency_data_pipeline`.`fb_ads_stats`

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
`journeyengine-recipes`.`agency_data_pipeline`.`gsc_stats`

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
`journeyengine-recipes`.`agency_data_pipeline`.`linkedin_adplatforms`