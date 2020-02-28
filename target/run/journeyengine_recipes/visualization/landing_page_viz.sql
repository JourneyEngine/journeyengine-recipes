

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`landing_page_viz`
  
  
  OPTIONS()
  as (
    SELECT 
date,
yyyymm Month, 
site Site,
platform Platform,
channel Channel, 
url URL,
ifnull(cost, 0) Cost,
ifnull(impressions, 0) Impressions,
ifnull(clicks, 0) Clicks,
ifnull(conversions, 0) Conversions,
ifnull(sessions, 0) Sessions,
ifnull(goal_completions, 0) GoalCompletions,
ifnull(conversion_index, 0) ConversionIndex,
ifnull(mqls, 0) MQLs,
ifnull(subscribers, 0) Subscribers,
ifnull(other, 0) OtherConversions,
ifnull(excluded, 0) ExcludedConversions,
FROM `journeyengine-recipes`.`agency_data_pipeline`.`landing_page_index`
  );
    