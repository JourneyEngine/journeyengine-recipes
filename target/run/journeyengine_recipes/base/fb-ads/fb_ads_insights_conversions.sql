

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`fb_ads_insights_conversions`
  
  
  OPTIONS()
  as (
    -- depends_on: `journeyengine-recipes`.`agency_data_pipeline`.`fb_conversions`





with fb_ads_insights_conversions as (

	    

	    	

		   	SELECT
		   	
  date_start
, campaign_id
, campaign_name
, ad_id
, account_name
, _sdc_sequence,
			value.action_type,
			value._28d_click conversions,
			null as revenue
			FROM `journeyengine-recipes.fb_ads_quantic.ads_insights`
			cross join unnest(actions)

			## goal completion columns
			
			
			UNION ALL

		   	SELECT 
			
  date_start
, campaign_id
, campaign_name
, ad_id
, account_name
, _sdc_sequence,
			value.action_type,
			null as conversions,
			value._28d_click as revenue
			FROM `journeyengine-recipes.fb_ads_quantic.ads_insights`
			cross join unnest(actions)
			
			## goal completion columns
			

		    
	   

)

SELECT 
date,
campaign_id,
campaign,
ad_id,
account,
sum(conversions) conversions,
sum(revenue) revenue
FROM 
		(
	    SELECT
	    date_start date,
	    campaign_id,
	    campaign_name campaign,
	    ad_id,
	    account_name account,
	    action_type,
	    conversions,
	    revenue,
	    first_value(_sdc_sequence) OVER (PARTITION BY ad_id, date_start ORDER BY _sdc_sequence DESC) lv,
	    _sdc_sequence
	    FROM fb_ads_insights_conversions
	   	)
where lv = _sdc_sequence
group by date, campaign_id, campaign, ad_id, account



  );
    