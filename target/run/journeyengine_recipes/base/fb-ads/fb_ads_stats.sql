

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`fb_ads_stats`
  
  
  OPTIONS()
  as (
    with cost as (

    select 
        date, account, ad_id, campaign,
        cost, impressions, clicks, null as conversions
    from `journeyengine-recipes`.`agency_data_pipeline`.`fb_ads_insights`
),

conversions as (

    select 
        date, account, ad_id, campaign,
        null as cost, null as impressions, null as clicks, conversions
    from `journeyengine-recipes`.`agency_data_pipeline`.`fb_ads_insights_conversions`
),

urls as (

    select ad_id, url
    from `journeyengine-recipes`.`agency_data_pipeline`.`fb_ads`
)


SELECT
    cast(date as date) date,
    'Facebook Ads' platform,
    'Paid' channel,
    account, 
    b.url url,
    campaign,
    sum(cost) cost,
    sum(impressions) impressions,
    sum(clicks) clicks,
    sum(conversions) conversions
FROM
(
    SELECT *
    FROM 
      cost
    UNION ALL
    SELECT *
    FROM 
      conversions
) a
LEFT JOIN urls b
ON ( a.ad_id = b.ad_id )
group by date, platform, channel, account, url, campaign
  );
    