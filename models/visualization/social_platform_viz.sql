SELECT
yyyymm Month, 
site Site,
platform Platform,
ifnull(posts, 0) Posts,
ifnull(impressions, 0) Impressions,
ifnull(reactions, 0) Reactions,
ifnull(comments, 0) Comments,
ifnull(shares, 0) Shares,
ifnull(link_clicks, 0) LinkClicks,
round(ifnull(impressions_per_post,0),0) ImpressionsPerPost,
round(ifnull(applause_rate, 0),0) ApplauseRate,
round(ifnull(conversation_rate, 0),0) ConversationRate,
round(ifnull(amplification_rate, 0),0) AmplificationRate,
round(ifnull(clicks_per_post, 0),0) ClicksPerPost
FROM {{ ref('social_platform_by_month') }}