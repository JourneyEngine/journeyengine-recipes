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
{{ ref('facebook_insights_stats') }} 

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
{{ ref('instagram_insights_stats') }} 

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
{{ ref('linkedin_company_pages_stats') }} 

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
{{ ref('twitter_premium_stats') }} 

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
{{ ref('youtube_insights_stats') }} 