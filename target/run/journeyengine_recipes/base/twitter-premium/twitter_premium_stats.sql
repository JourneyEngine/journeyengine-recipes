

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`twitter_premium_stats`
  
  
  OPTIONS()
  as (
    select
platform,
account, 
date, 
null as post_image_url, 
post_url, 
post_text,
impressions, 
reactions, 
comments, 
shares,
0 as link_clicks
from (
  select
  'Twitter' as platform,
  account, 
  date, 
  post_url,
  null as post_image_url,
  post_text, 
  impressions, 
  reactions, 
  comments, 
  shares,
  0 as link_clicks, 
  time_of_entry,
  max(time_of_entry) over (partition by date, post_url) lv
  from `journeyengine-recipes.agency_data_pipeline.twitter_premium` 
)
where 
account is not null and
time_of_entry = lv
  );
    