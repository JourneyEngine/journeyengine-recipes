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
sum(sessions) OVER w1 total_sessions,
goal_completions,
sum(goal_completions) OVER w1 total_goal_completions,
mqls,
sum(mqls) OVER w1 total_mqls,
sqls,
sum(sqls) OVER w1 total_sqls,
other,
sum(other) OVER w1 total_other,
excluded,
sum(excluded) OVER w1 total_excluded,
FROM (
    SELECT 
    date,
    FORMAT_DATE("%Y-%m", date) AS yyyymm,
    site,
    platform,
    channel, 
    sum(cost) cost,
    sum(impressions) impressions,
    sum(clicks) clicks,
    sum(conversions) conversions,
    sum(sessions) sessions,
    sum(goal_completions) goal_completions,
    sum(mqls) mqls,
    sum(sqls) sqls,
    sum(other) other,
    sum(excluded) excluded
    FROM {{ ref('agg_platforms_ga') }}
    GROUP BY date, yyyymm, site, channel, platform
)
WINDOW w1 as (PARTITION BY yyyymm, site)