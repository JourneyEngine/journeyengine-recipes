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
  first_value(time_of_entry) over (partition by account, date, post_url, post_text order by time_of_entry desc) lv
  from `{{ target.project }}.agency_data_pipeline.linkedin_company_pages`
)
where time_of_entry = lv