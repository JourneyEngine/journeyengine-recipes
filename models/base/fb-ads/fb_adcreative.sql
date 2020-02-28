{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Facebook Ads') %}

{% if accounts != [] %}

with fb_adcreative as (


      {% for account in accounts %}
        SELECT 
        {{ fill('fb_ads', 'adcreative', account,
        ['id',
        'object_url',
      'url_tags',
      '_sdc_sequence']) }}
            FROM  {{ most_recent_record(
                        relation='`' ~ target.project ~ '.fb_ads_' ~ account ~ '.adcreative`',
                        pk_fields=['id']
                  )}}
        {% if not loop.last %} UNION ALL {% endif %}
     {% endfor %}
)

select creative_id, max(url) url
from (
  select
    id creative_id,
    lower(trim(regexp_replace(replace(replace(replace(replace(object_url,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url
  from fb_adcreative
  )
group by creative_id

{% endif %}