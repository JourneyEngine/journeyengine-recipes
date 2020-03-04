

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`landing_page_index`
  
  
  OPTIONS()
  as (
    SELECT 
date,
yyyymm, 
site,
platform,
channel, 
url,
cost,
impressions,
clicks,
conversions,
sessions,
pct_sessions,
goal_completions,
pct_goal_completions,
mqls,
pct_mqls,
sqls,
pct_sqls,
other,
excluded,
case when pct_sessions > 0 then pct_goal_completions / pct_sessions else null end as conversion_index
FROM (
    SELECT
    date,
    yyyymm, 
    site,
    platform,
    channel, 
    url,
    cost,
    impressions,
    clicks,
    conversions,
    sessions,
    case when total_sessions > 0 then sessions / total_sessions else null end as pct_sessions,
    goal_completions,
    case when total_goal_completions > 0 then goal_completions / total_goal_completions else null end as pct_goal_completions,
    mqls,
    case when total_mqls > 0 then mqls / total_mqls else null end as pct_mqls,
    sqls,
    case when total_sqls > 0 then sqls / total_sqls else null end as pct_sqls,
    other,
    excluded
    FROM `journeyengine-recipes`.`agency_data_pipeline`.`landing_page_by_month`
)
  );
    