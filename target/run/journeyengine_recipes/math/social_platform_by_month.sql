

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`social_platform_by_month`
  
  
  OPTIONS()
  as (
    SELECT
yyyymm, 
site,
platform,
posts,
impressions, 
reactions, 
comments, 
shares,
link_clicks,
sum(impressions/posts) OVER w1 impressions_per_post,
sum(reactions/posts) OVER w1 applause_rate,
sum(comments/posts) OVER w1 conversation_rate,
sum(comments/posts) OVER w1 amplification_rate,
sum(link_clicks/posts) OVER w1 clicks_per_post

FROM (
    SELECT 
    FORMAT_DATE("%Y-%m", date) AS yyyymm,
    site,
    platform, 
    sum(impressions) impressions,
    sum(reactions) reactions,
    sum(comments) comments,
    sum(shares) shares,
    sum(link_clicks) link_clicks,
    count(post_url) as posts
	FROM `journeyengine-recipes`.`agency_data_pipeline`.`agg_social_platforms_site`
    GROUP BY yyyymm, site, platform
    )
 WINDOW w1 as (PARTITION BY yyyymm, site)
  );
    