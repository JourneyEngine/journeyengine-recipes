with ga as (

	select 
		date, account, site, channel, platform, url,
		sessions, goal_completions, mqls, subscribers, other, excluded,
		null as cost, null as impressions, null as clicks, null as conversions
	from `journeyengine-recipes`.`agency_data_pipeline`.`ga_stats`

),

platforms as (

	select 
		date, account, site, channel, platform, url,
		null as sessions, null as goal_completions, null as mqls, null as subscribers, null as other, null as excluded,
		cost, impressions, clicks, conversions
	from `journeyengine-recipes`.`agency_data_pipeline`.`agg_platforms_site`

)


SELECT
    date, 
    site,
    platform,
    channel, 
    url,
    sum(cost) cost,
	sum(impressions) impressions,
	sum(clicks) clicks,
	sum(conversions) conversions,
	sum(sessions) sessions,
	sum(goal_completions) goal_completions,
	sum(mqls) mqls,
	sum(subscribers) subscribers,
	sum(other) other,
	sum(excluded) excluded
FROM
(
    SELECT *
    FROM 
      ga
    UNION ALL
    SELECT *
    FROM 
      platforms 
) 
GROUP BY
    date, site, platform, channel, url