

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`linkedin_company_pages_stats`
  
  
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
from (
  select
  'LinkedIn' as platform,
  account, 
  date, 
  post_url,
  post_image_url,
  post_text, 
  impressions, 
  reactions, 
  comments, 
  shares,
  link_clicks,
  time_of_entry,
  max(time_of_entry) over (partition by date, post_url) lv
  from `journeyengine-recipes.agency_data_pipeline.linkedin_company_pages`
)
where time_of_entry = lv
  );
    