



with adwords_campaigns as (
      
 
	SELECT  
	'Paid' as channel,
	'Google Ads' as platform,
	day,
	campaignid,
	campaign,
	account as account,
	cost/1000000 cost, 
	impressions as impressions, 
	clicks, 
	conversions,
	_sdc_sequence,
	first_value(_sdc_sequence) OVER (PARTITION BY campaignid, day ORDER BY _sdc_sequence DESC) lv
	FROM `journeyengine-recipes.google_ads_quantic.CAMPAIGN_PERFORMANCE_REPORT`
	    	
            
)

SELECT
day,
campaignid,
campaign,
account, 
channel,
platform,
sum(cost) cost,
sum(impressions) impressions,
sum(clicks) clicks,
sum(conversions) conversions
FROM adwords_campaigns

WHERE lv = _sdc_sequence
GROUP BY day, campaignid, account, channel, platform, campaign

 