

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`agg_adplatforms_union`
  
  
  OPTIONS()
  as (
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

`journeyengine-recipes`.`agency_data_pipeline`.`fb_adplatforms`

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

`journeyengine-recipes`.`agency_data_pipeline`.`adwords_adplatforms`

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

`journeyengine-recipes`.`agency_data_pipeline`.`linkedin_adplatforms`

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

`journeyengine-recipes`.`agency_data_pipeline`.`twitter_adplatforms`
  );
    