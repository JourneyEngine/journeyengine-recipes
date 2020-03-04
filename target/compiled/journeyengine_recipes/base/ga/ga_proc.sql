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
			
				
					cast(goal19completions as int64) 
					 
					 as mqls,  
				
			
			
				
					cast(goal8completions as int64) 
					 +  
					 
				
					cast(goal9completions as int64) 
					 
					 as sqls,  
				
						
							
				null as excluded,		
					
			
				
					cast(goal17completions as int64) 
					 +  
					 
				
					cast(goal16completions as int64) 
					 +  
					 
				
					cast(goal18completions as int64) 
					 +  
					 
				
					cast(goal13completions as int64) 
					 
					 as other,  
				
						
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
sum(sqls) sqls,
sum(excluded) excluded,
sum(other) other
FROM ga_report
where lv = _sdc_sequence
group by bigquery_name, lookup_platform, date, url, source, medium

