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
0 as shares,
0 as link_clicks
from (
  select
  'Instagram' as platform,
  account, 
  date, 
  post_url,
  post_image_url,
  post_text, 
  impressions, 
  reactions, 
  comments, 
  0 as shares,
  0 as link_clicks, 
  time_of_entry,
  max(time_of_entry) over (partition by date, post_url order by time_of_entry desc) lv
  from `{{ target.project }}.agency_data_pipeline.instagram_insights` 
)
where time_of_entry = lv