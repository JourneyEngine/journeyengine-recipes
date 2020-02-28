
{% set accounts = get_column_values(table=ref('accounts_proc'), column='bigquery_name', max_records=50, filter_column='platform', filter_value='Facebook Ads') %}

{% if accounts != [] %}

with fb_adplatforms as (

      {% for account in accounts %}
          
          select
          account_name account,
          'Facebook Ads' as platform,
          date_start date,
          campaign_name campaign,
          adset_name group_set,
          ad_name ad,
          objective,
          Concat(publisher_platform,"-",platform_position) as placement,
          spend,
          impressions,
          clicks,
          inline_link_clicks link_clicks,
          sum(_28d_click) as conversions

          from (

                select
                {{ fill('fb_ads', 'ads_insights_platform_and_device', account,
                ['account_name',
                'date_start',
                'campaign_name',
                'adset_name',
                'ad_name',
                'objective',
                'publisher_platform',
                'platform_position',
                'spend',
                'impressions',
                'clicks',
                'inline_link_clicks',
                '_sdc_sequence',
                '_sdc_batched_at']) }},
                value._28d_click,
               from {{ most_recent_record(
                        relation='`' ~ target.project ~ '.fb_ads_' ~ account ~ '.ads_insights_platform_and_device`',
                        pk_fields=['ad_id', 'adset_id', 'campaign_id', 'date_start', 'publisher_platform', 'platform_position', 'impression_device']
                  )}}
               left join unnest(actions)

               )

            {{ dbt_utils.group_by(12) }}

                      {% if not loop.last %} UNION ALL {% endif %}
                   {% endfor %}

)

select distinct
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
from fb_adplatforms

 {% endif %}