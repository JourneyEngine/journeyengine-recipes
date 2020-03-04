

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`platform_index`
  
  
  OPTIONS()
  as (
    SELECT 
date,
yyyymm, 
site,
platform,
channel, 
cost,
impressions,
clicks,
conversions,
sessions,
goal_completions,
mqls,
sqls,
other,
excluded,
FROM (
    SELECT
    date,
    yyyymm, 
    site,
    platform,
    channel, 
    cost,
    impressions,
    clicks,
    conversions,
    sessions,
    goal_completions,
    mqls,
    sqls,
    other,
    excluded,
    FROM `journeyengine-recipes`.`agency_data_pipeline`.`platform_by_month`
)
  );
    