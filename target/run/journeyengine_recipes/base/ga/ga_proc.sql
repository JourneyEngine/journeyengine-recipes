

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`ga_proc`
  
  
  OPTIONS()
  as (
    -- depends_on: `journeyengine-recipes`.`agency_data_pipeline`.`ga_conversions`




with ga_report as (

	    

	    	
	    	
	    	
	    	

		   	SELECT
		   	'quantic' as bigquery_name,
		   	'Google Analytics' as lookup_platform,
			lower(trim(regexp_replace(replace(replace(replace(replace(CONCAT(hostname,landingpagepath),'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
			date,
			lower(source) source,
			lower(medium) medium,
			sessions,
			goalcompletionsall goal_completions,
			## goal completion columns
							
				null as mqls,		
			
							
				null as subscribers,		
						
							
				null as excluded,		
					
							
				null as other,		
						
			_sdc_sequence,
			first_value(_sdc_sequence) OVER (PARTITION BY hostname, landingpagepath, date, source, medium ORDER BY _sdc_sequence DESC) lv
			FROM `journeyengine-recipes.ga_quantic.report` 

		    
	   

)


SELECT 
bigquery_name,
lookup_platform,
date,
url,
source,
medium,
sum(sessions) sessions,
sum(goal_completions) goal_completions,
sum(mqls) mqls,
sum(subscribers) subscribers,
sum(excluded) excluded,
sum(other) other
FROM ga_report
where lv = _sdc_sequence
group by bigquery_name, lookup_platform, date, url, source, medium


  );
    