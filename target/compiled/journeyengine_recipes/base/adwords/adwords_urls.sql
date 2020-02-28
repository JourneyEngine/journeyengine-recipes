



with adwords_urls as (
      

	SELECT  
	campaignid,
	max(finalurl) finalurl
	FROM `journeyengine-recipes.google_ads_quantic.FINAL_URL_REPORT`
	GROUP BY campaignid
		    
            
)

SELECT
campaignid,
lower(trim(regexp_replace(replace(replace(replace(replace(finalurl,'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url
FROM adwords_urls

 