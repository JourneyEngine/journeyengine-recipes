

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`adwords_join`
  
  
  OPTIONS()
  as (
    SELECT
day,
a.campaign,
a.campaignid, 
account, 
channel,
platform,
b.url,
cost,
impressions,
clicks,
conversions
FROM `journeyengine-recipes`.`agency_data_pipeline`.`adwords_campaigns` a
LEFT JOIN `journeyengine-recipes`.`agency_data_pipeline`.`adwords_urls` b
ON a.campaignid = b.campaignid
  );
    