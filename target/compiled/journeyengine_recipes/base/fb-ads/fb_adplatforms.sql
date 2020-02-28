



with fb_adplatforms as (

      
          
          select
          account_name account,
          'Facebook Ads' as platform,
          date_start date,
          campaign_name campaign,
          adset_name group_set,
          ad_name ad,
          objective,
          Concat(publisher_platform,"-",platform_position) as placement,
          spend,
          impressions,
          clicks,
          inline_link_clicks link_clicks,
          sum(_28d_click) as conversions

          from (

                select
                
  account_name
, date_start
, campaign_name
, adset_name
, ad_name
, objective
, publisher_platform
, platform_position
, spend
, impressions
, clicks
, inline_link_clicks
, _sdc_sequence
, _sdc_batched_at,
                value._28d_click,
               from 



(
	select * from (

		select 
			*,
			max(_sdc_sequence) over (partition by ad_id,adset_id,campaign_id,date_start,publisher_platform,platform_position,impression_device) as most_recent_sequence,
			max(_sdc_batched_at) over (partition by ad_id,adset_id,campaign_id,date_start,publisher_platform,platform_position,impression_device) as most_recent_batch
		from `journeyengine-recipes.fb_ads_quantic.ads_insights_platform_and_device` 

	) 
	where _sdc_sequence = most_recent_sequence
	and _sdc_batched_at = most_recent_batch

)


               left join unnest(actions)

               )

            group by 1,2,3,4,5,6,7,8,9,10,11,12

                      
                   

)

select distinct
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
from fb_adplatforms

 