CREATE OR REPLACE PROCEDURE dwh.usptl_dim_networks_subscriptors()
	LANGUAGE plpgsql
AS $$
	
	
	
BEGIN

	--identify unic clients
	CREATE TEMP TABLE tmp_networks_subscriptors AS 
	SELECT DISTINCT 
		   mnc
		 , imsi_id  
	FROM stg.cliente_raw_data;
	
	CREATE TEMP TABLE stage(LIKE dwh.dim_networks_subscriptors);
	
	INSERT INTO stage
	SELECT
		  dim.id_network_subscriptor
		, stg.mnc AS cod_mnc_network
		, stg.imsi_id AS cod_imsi_network_subscriptor
	FROM tmp_networks_subscriptors AS stg
    INNER JOIN dwh.dim_networks_subscriptors AS dim 
    	ON dim.cod_mnc_network = stg.mnc
    	AND dim.cod_imsi_network_subscriptor = stg.imsi_id;

    
  	--insert old rows (UPDATE)	
	DELETE FROM dwh.dim_networks_subscriptors
  	USING stage
  	WHERE dwh.dim_networks_subscriptors.id_network_subscriptor = stage.id_network_subscriptor;

    INSERT INTO dwh.dim_networks_subscriptors 
    (
		  id_network_subscriptor
		, cod_mnc_network
		, cod_imsi_network_subscriptor
		, load_date
		, updated_at
	)
 	SELECT 		 
 		  id_network_subscriptor
 		, cod_mnc_network
 		, cod_imsi_network_subscriptor
 		, current_timestamp
 		, current_timestamp
	FROM stage;

	drop table stage;
 

    
   --insert new rows
 	INSERT INTO dwh.dim_networks_subscriptors
	(
		  id_network_subscriptor
		, cod_mnc_network
		, cod_imsi_network_subscriptor
		, load_date
		, updated_at
	)
	SELECT
		  CAST(COALESCE(dim.id_network_subscriptor, SUM(NVL2(dim.id_network_subscriptor, 0, 1)) OVER(ORDER BY stg.mnc, imsi_id ROWS UNBOUNDED PRECEDING) + MAXID.ID) AS INTEGER ) AS id_network_subscriptor
		, stg.mnc AS cod_mnc_network
		, stg.imsi_id as cod_imsi_network_subscriptor
		, current_timestamp
		, current_timestamp
	FROM tmp_networks_subscriptors AS stg
	--JOIN ESTRUCTURA MAX_IDS
	CROSS JOIN ( SELECT COALESCE(MAX(id_network_subscriptor),0) AS ID FROM dwh.dim_networks_subscriptors) MAXID--autogenerado
    LEFT JOIN dwh.dim_networks_subscriptors AS dim 
    	ON dim.cod_mnc_network = stg.mnc
    	AND dim.cod_imsi_network_subscriptor = stg.imsi_id 
    WHERE dim.id_network_subscriptor IS NULL;
    
   
    drop table tmp_networks_subscriptors;
   
   
END;




$$
;
