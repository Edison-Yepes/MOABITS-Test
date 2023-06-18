CREATE OR REPLACE PROCEDURE dwh.usptl_dim_countries()
	LANGUAGE plpgsql
AS $$
	
	
	
BEGIN

	--identify unic clients
	CREATE TEMP TABLE tmp_countries AS 
	SELECT DISTINCT 
		   country_iso3
		 , mcc 
		 , country_name 
	FROM stg.cliente_raw_data;
	
	CREATE TEMP TABLE stage(LIKE dwh.dim_countries);
	
	INSERT INTO stage
	SELECT
		  dim.id_country
		, stg.country_iso3 AS cod_iso3_country
		, stg.mcc AS cod_mcc_country
		, stg.country_name
	FROM tmp_countries AS stg
    INNER JOIN dwh.dim_countries AS dim 
    	ON dim.cod_iso3_country = stg.country_iso3;

    
  	--insert old rows (UPDATE)	
	DELETE FROM dwh.dim_countries
  	USING stage
  	WHERE dwh.dim_countries.id_country = stage.id_country;

    INSERT INTO dwh.dim_countries 
    (
		  id_country
		, cod_iso3_country
		, cod_mcc_country
		, country_name
		, load_date
		, updated_at
	)
 	SELECT 		 
 		  id_country
 		, cod_iso3_country
 		, cod_mcc_country
 		, country_name
 		, current_timestamp
 		, current_timestamp
	FROM stage;

	drop table stage;
 

    
   --insert new rows
 	INSERT INTO dwh.dim_countries
	(
		  id_country
		, cod_iso3_country
		, cod_mcc_country
		, country_name
		, load_date
		, updated_at
	)
	SELECT
		  CAST(COALESCE(dim.id_country, SUM(NVL2(dim.id_country, 0, 1)) OVER(ORDER BY stg.country_iso3 ROWS UNBOUNDED PRECEDING) + MAXID.ID) AS INTEGER ) AS id_country
		, stg.country_iso3 AS cod_iso3_country
		, stg.mcc as cod_mcc_country
		, stg.country_name
		, current_timestamp
		, current_timestamp
	FROM tmp_countries AS stg
	--JOIN ESTRUCTURA MAX_IDS
	CROSS JOIN ( SELECT COALESCE(MAX(id_country),0) AS ID FROM dwh.dim_countries) MAXID--autogenerado
    LEFT JOIN dwh.dim_countries AS dim 
    	ON dim.cod_iso3_country = stg.country_iso3
    WHERE dim.id_country IS NULL;
    
   
    drop table tmp_countries;
   
   
END;




$$
;
