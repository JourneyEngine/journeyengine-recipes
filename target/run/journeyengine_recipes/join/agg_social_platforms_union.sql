

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`agg_social_platforms_union`
  
  
  OPTIONS()
  as (
    select
platform,
account, 
date, 
post_image_url, 
post_url, 
post_text,
impressions, 
reactions, 
comments, 
shares,
link_clicks
from 
`journeyengine-recipes`.`agency_data_pipeline`.`facebook_insights_stats` 

UNION ALL
select
platform,
account, 
date, 
post_image_url, 
post_url, 
post_text,
impressions, 
reactions, 
comments, 
shares,
link_clicks
from 
`journeyengine-recipes`.`agency_data_pipeline`.`instagram_insights_stats` 

UNION ALL

select
platform,
account, 
date, 
post_image_url, 
post_url, 
post_text,
impressions, 
reactions, 
comments, 
shares,
link_clicks
from 
`journeyengine-recipes`.`agency_data_pipeline`.`linkedin_company_pages_stats` 

UNION ALL
select
platform,
account, 
date, 
cast (post_image_url as string), 
post_url, 
post_text,
impressions, 
reactions, 
comments, 
shares,
link_clicks
from 
`journeyengine-recipes`.`agency_data_pipeline`.`twitter_premium_stats` 

UNION ALL
select
platform,
account, 
date, 
cast (post_image_url as string), 
post_url, 
post_text,
impressions, 
reactions, 
comments, 
shares,
link_clicks
from 
`journeyengine-recipes`.`agency_data_pipeline`.`youtube_insights_stats`
  );
    