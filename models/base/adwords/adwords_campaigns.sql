{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Google Ads') %}

{% if accounts != [] %}

with adwords_campaigns as (
      {% for account in accounts %}
 
	SELECT  
	'Paid' as channel,
	'Google Ads' as platform,
	day,
	campaignid,
	campaign,
	account as account,
	cost/1000000 cost, 
	impressions as impressions, 
	clicks, 
	conversions,
	_sdc_sequence,
	first_value(_sdc_sequence) OVER (PARTITION BY campaignid, day ORDER BY _sdc_sequence DESC) lv
	FROM `{{ target.project }}.google_ads_{{account}}.CAMPAIGN_PERFORMANCE_REPORT`
	    	{% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
)

SELECT
day,
campaignid,
campaign,
account, 
channel,
platform,
sum(cost) cost,
sum(impressions) impressions,
sum(clicks) clicks,
sum(conversions) conversions
FROM adwords_campaigns

WHERE lv = _sdc_sequence
GROUP BY day, campaignid, account, channel, platform, campaign

 {% endif %}