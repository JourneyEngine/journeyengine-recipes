

  create or replace table `journeyengine-recipes`.`agency_data_pipeline`.`fb_adcreative`
  
  
  OPTIONS()
  as (
    



with fb_adcreative as (


      
        SELECT 
        
  id
, object_url
, url_tags
, _sdc_sequence
            FROM  



(
	select * from (

		select 
			*,
			max(_sdc_sequence) over (partition by id) as most_recent_sequence,
			max(_sdc_batched_at) over (partition by id) as most_recent_batch
		from `journeyengine-recipes.fb_ads_quantic.adcreative` 

	) 
	where _sdc_sequence = most_recent_sequence
	and _sdc_batched_at = most_recent_batch

)


        
     
)

select creative_id, max(url) url
from (
  select
    id creative_id,
    lower(trim(regexp_replace(replace(replace(replace(replace(object_url,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url
  from fb_adcreative
  )
group by creative_id


  );
    