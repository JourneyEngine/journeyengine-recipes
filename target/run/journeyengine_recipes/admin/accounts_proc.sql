

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`accounts_proc`
  
  
  OPTIONS()
  as (
    SELECT
site,
account,
platform,
bigquery_name
FROM (
  SELECT
  site,
  account,
  platform,
  bigquery_name,
  time_of_entry,
  first_value(time_of_entry) over (order by time_of_entry desc) lv
  FROM `journeyengine-recipes.agency_data_pipeline.accounts`
  )
  WHERE time_of_entry = lv
  );
    