with cost as (

    select 
        date, account, ad_id, campaign,
        cost, impressions, clicks, null as conversions
    from {{ref('fb_ads_insights')}}
),

conversions as (

    select 
        date, account, ad_id, campaign,
        null as cost, null as impressions, null as clicks, conversions
    from {{ref('fb_ads_insights_conversions')}}
),

urls as (

    select ad_id, url
    from {{ref('fb_ads')}}
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