SELECT 
date,
yyyymm Month, 
site Site,
platform Platform,
channel Channel, 
ifnull(cost, 0) Cost,
ifnull(impressions, 0) Impressions,
ifnull(clicks, 0) Clicks,
ifnull(conversions, 0) Conversions,
ifnull(sessions, 0) Sessions,
ifnull(goal_completions, 0) GoalCompletions,
ifnull(mqls, 0) MQLs,
ifnull(subscribers, 0) Subscribers,
ifnull(other, 0) OtherConversions,
ifnull(excluded, 0) ExcludedConversions,
FROM `journeyengine-recipes`.`agency_data_pipeline`.`platform_index`
WHERE yyyymm >= '2017-06'