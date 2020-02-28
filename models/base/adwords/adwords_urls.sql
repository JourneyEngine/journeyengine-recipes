{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Google Ads') %}

{% if accounts != [] %}

with adwords_urls as (
      {% for account in accounts %}

	SELECT  
	campaignid,
	max(finalurl) finalurl
	FROM `{{ target.project }}.google_ads_{{account}}.FINAL_URL_REPORT`
	GROUP BY campaignid
		    {% if not loop.last %} UNION ALL {% endif %}
            {% endfor %}
)

SELECT
campaignid,
lower(trim(regexp_replace(replace(replace(replace(replace(finalurl,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url
FROM adwords_urls

 {% endif %}