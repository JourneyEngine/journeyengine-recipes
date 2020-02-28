

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`adplatform_cost_calculations`
  
  
  OPTIONS()
  as (
    SELECT
date,
account,
site,
platform,
campaign,
group_set,
ad,
objective,
placement,
spend,
impressions,
clicks,
link_clicks,
conversions,
cpm,
cpc,
cpc_link,
cpconversion
FROM(
		SELECT
		date,
		account,
		site,
		platform,
		campaign,
		group_set,
		ad,
		objective,
		placement,
		spend,
		impressions,
		clicks,
		link_clicks,
		conversions,
		case when spend > 0 and impressions > 0 then spend / (impressions/1000) else null end as cpm,
		case when spend > 0 and clicks > 0 then spend / clicks else null end as cpc,
		case when spend > 0 and link_clicks > 0 then spend / link_clicks else null end as cpc_link,
		case when spend > 0 and conversions > 0 then spend / conversions else null end as cpconversion
		FROM `journeyengine-recipes`.`agency_data_pipeline`.`agg_adplatforms_site`
	)
  );
    