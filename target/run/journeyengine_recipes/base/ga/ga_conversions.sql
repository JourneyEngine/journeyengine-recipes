

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`ga_conversions`
  
  
  OPTIONS()
  as (
    SELECT 
site,
bigquery_name,
platform,
goal_name,
goal_type,
account,
time_of_entry
FROM `journeyengine-recipes`.`agency_data_pipeline`.`conversion_goals_proc`
WHERE platform = 'Google Analytics'
  );
    