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
  {{ref('agg_social_platforms_union')}} a
LEFT JOIN 
  {{ref('accounts_proc')}} b
ON ( 
  a.account = b.account AND
  a.platform = b.platform
)