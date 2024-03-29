



with fb_ads_insights as (

	    
	    
		   	SELECT 
			date_start,
			campaign_id,
			campaign_name,
			ad_id,
			account_name,
			spend,
			reach,
			inline_link_clicks,
			_sdc_sequence,
			first_value(_sdc_sequence) OVER (PARTITION BY date_start, ad_id, campaign_id ORDER BY _sdc_sequence DESC) lv

			from (
				select
				
  date_start
, campaign_id
, campaign_name
, ad_id
, account_name
, spend
, null as reach
, inline_link_clicks
, _sdc_sequence,
			FROM `journeyengine-recipes.fb_ads_quantic.ads_insights`

			)

		    
	   

)

select
date_start date,
campaign_id,
campaign_name campaign,
ad_id,
account_name account,
max(spend) cost,
max(reach) impressions,
max(inline_link_clicks) clicks
from fb_ads_insights
where lv = _sdc_sequence
group by date, campaign_id, ad_id, account, campaign

