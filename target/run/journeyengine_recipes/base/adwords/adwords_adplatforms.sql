

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`adwords_adplatforms`
  
  
  OPTIONS()
  as (
    



with adwords_adplatforms as (

      
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
         from `journeyengine-recipes.google_ads_quantic.AD_PERFORMANCE_REPORT`
                
             

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

 
  );
    