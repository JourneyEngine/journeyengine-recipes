SELECT
date, 
account, 
channel,
platform,
url,
keyword,
0 as cost,
sum(impressions) as impressions,
sum(clicks) as clicks,
0 as conversions
FROM 
  ( 
	SELECT  
	date, 
	site as account,
	'Organic' as channel,
	'Organic' as platform,
	lower(trim(regexp_replace(replace(replace(replace(replace(landing_page_url,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
	keyword,
	max(impressions) as impressions, 
	max(clicks) as clicks,
	FROM `journeyengine-recipes.agency_data_pipeline.gsc`
	GROUP BY date, account, channel, platform, url, keyword
  ) 
GROUP BY date, account, channel, platform, url, keyword
ORDER BY account asc, date asc, url asc