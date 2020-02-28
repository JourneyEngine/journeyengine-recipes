{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Google Ads') %}

{% if accounts != [] %}

with adwords_adplatforms as (

      {% for account in accounts %}
          select
          account,
          'Google Ads' as platform,
          day date,
          campaign,
          adgroup group_set,
          concat( headline1, " | ", headline2, " - ", description) as ad,
          null as objective,
          adtype placement,
          cost / 1000000 as spend,
          impressions,
          clicks,
          clicks link_clicks,
          conversions,
          _sdc_report_datetime,
          first_value(_sdc_report_datetime) over (partition by day order by _sdc_report_datetime desc) lv
         from `{{ target.project }}.google_ads_{{account}}.AD_PERFORMANCE_REPORT`
                {% if not loop.last %} UNION ALL {% endif %}
             {% endfor %}

)

 select
account,
platform,
date,
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
from adwords_adplatforms
 where _sdc_report_datetime = lv

 {% endif %}