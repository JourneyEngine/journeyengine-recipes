

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`agg_social_platforms_site`
  
  
  OPTIONS()
  as (
    SELECT 
date, 
a.account account, 
b.site site,
a.platform platform,
post_image_url, 
post_url, 
post_text,
impressions, 
reactions, 
comments, 
shares,
link_clicks
FROM 
  `journeyengine-recipes`.`agency_data_pipeline`.`agg_social_platforms_union` a
LEFT JOIN 
  `journeyengine-recipes`.`agency_data_pipeline`.`accounts_proc` b
ON ( 
  a.account = b.account AND
  a.platform = b.platform
)
  );
    