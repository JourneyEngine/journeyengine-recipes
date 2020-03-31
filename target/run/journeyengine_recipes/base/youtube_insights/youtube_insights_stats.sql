

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`youtube_insights_stats`
  
  
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
link_clicks
from (
  select
  'YouTube' as platform,
  account, 
  date, 
  post_url,
  null as post_image_url,
  post_text, 
  views as impressions, 
  reactions, 
  comments, 
  shares,
  link_clicks, 
  time_of_entry,
  max(time_of_entry) over (partition by date, post_url) lv
  from `journeyengine-recipes.agency_data_pipeline.youtube_insights`
)
where time_of_entry = lv
  );
    