SELECT 
date, 
a.account account, 
b.site site,
a.platform platform,
campaign, 
group_set, 
ad,
objective,
placement, 
spend,
impressions, 
clicks,
link_clicks,
conversions
FROM 
  {{ref('agg_adplatforms_union')}} a
LEFT JOIN 
  {{ref('accounts_proc')}} b
ON ( 
  a.account = b.account AND
  a.platform = b.platform
)