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
  first_value(time_of_entry) over (partition by account, date, post_url, post_text order by time_of_entry desc) lv
  from `journeyengine-recipes.agency_data_pipeline.twitter_premium` 
)
where 
account is not null and
time_of_entry = lv