-- depends_on: `journeyengine-recipes`.`agency_data_pipeline`.`fb_adcreative`





with fb_ads as (

	    
		   	SELECT 
			id ad_id,
			creative.id creative_id
			FROM  



(
	select * from (

		select 
			*,
			max(_sdc_sequence) over (partition by id) as most_recent_sequence,
			max(_sdc_batched_at) over (partition by id) as most_recent_batch
		from `journeyengine-recipes.fb_ads_quantic.ads` 

	) 
	where _sdc_sequence = most_recent_sequence
	and _sdc_batched_at = most_recent_batch

)


		     
	   

)

SELECT
a.ad_id,
b.creative_id,
b.url
FROM (
    SELECT
    ad_id,
    creative_id,
    FROM fb_ads
    ) a
LEFT JOIN `journeyengine-recipes`.`agency_data_pipeline`.`fb_adcreative` b
ON (
	a.creative_id = b.creative_id 
)

