

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`agg_platforms_site`
  
  
  OPTIONS()
  as (
    SELECT 
date, 
a.account account, 
b.site site,
a.platform platform,
a.channel channel,
url,
cost,
impressions,
clicks,
conversions
FROM 
  `journeyengine-recipes`.`agency_data_pipeline`.`agg_platforms_union` a
LEFT JOIN 
  `journeyengine-recipes`.`agency_data_pipeline`.`accounts_proc` b
ON ( 
  a.account = b.account AND
  a.platform = b.platform
)
  );
    