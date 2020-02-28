SELECT 
date,
site,
platform,
ifnull(post_image_url,'') ImageURL, 
ifnull(post_url,'') PostURL,
ifnull(post_text,'') PostText,
ifnull(impressions, 0) Impressions,
ifnull(reactions, 0) Reactions, 
ifnull(comments, 0) Comments, 
ifnull(shares, 0) Shares,
ifnull(link_clicks, 0) LinkClicks
FROM {{ ref('agg_social_platforms_site') }}