  -- depends_on: {{ ref('fb_adcreative') }}

{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Facebook Ads') %}

{% if accounts != [] %}

with fb_ads as (

	    {% for account in accounts %}
		   	SELECT 
			id ad_id,
			creative.id creative_id
			FROM  {{ most_recent_record(
                        relation='`' ~ target.project ~ '.fb_ads_' ~ account ~ '.ads`',
                        pk_fields=['id']
                  )}}
		     {% if not loop.last %} UNION ALL {% endif %}
	   {% endfor %}

)

SELECT
a.ad_id,
b.creative_id,
b.url
FROM (
    SELECT
    ad_id,
    creative_id,
    FROM fb_ads
    ) a
LEFT JOIN {{ref('fb_adcreative')}} b
ON (
	a.creative_id = b.creative_id 
)

{% endif %}