CREATE OR REPLACE PROCEDURE dwh.usptl_dim_clients()
	LANGUAGE plpgsql
AS $$
	
	
	
BEGIN

	--identify unic clients
	CREATE TEMP TABLE tmp_clients AS 
	SELECT DISTINCT 
		 INVENTORY_ID
		, COMPANY_NAME
	FROM stg.cliente_raw_data;
	
	CREATE TEMP TABLE stage(LIKE dwh.dim_clients);
	
	INSERT INTO stage
	SELECT
		  dim.id_client
		, stg.INVENTORY_ID AS cod_client
		, stg.COMPANY_NAME AS name_client
	FROM tmp_clients AS stg
    INNER JOIN dwh.dim_clients AS dim 
    	ON dim.cod_client = stg.INVENTORY_ID;

    
  	--insert old rows (UPDATE)	
	DELETE FROM dwh.dim_clients
  	USING stage
  	WHERE dwh.dim_clients.id_client = stage.id_client;

    INSERT INTO dwh.dim_clients  
    (
		  id_client
		, cod_client
		, name_client
		, load_date
		, updated_at
	)
 	SELECT 		 
 		  id_client
 		, cod_client
 		, name_client
 		, current_timestamp
 		, current_timestamp
	FROM stage;

	drop table stage;
 

    
   --insert new rows
 	INSERT INTO dwh.dim_clients
	(
		  id_client
		, cod_client
		, name_client
		, load_date
		, updated_at
	)
	SELECT
		  CAST(COALESCE(dim.id_client, SUM(NVL2(dim.id_client, 0, 1)) OVER(ORDER BY stg.INVENTORY_ID ROWS UNBOUNDED PRECEDING) + MAXID.ID) AS INTEGER ) AS id_client
		, stg.INVENTORY_ID
		, stg.COMPANY_NAME
		, current_timestamp
		, current_timestamp
	FROM tmp_clients AS stg
	--JOIN ESTRUCTURA MAX_IDS
	CROSS JOIN ( SELECT COALESCE(MAX(id_client),0) AS ID FROM dwh.dim_clients) MAXID--autogenerado
    LEFT JOIN dwh.dim_clients AS dim 
    	ON dim.cod_client = stg.INVENTORY_ID
    WHERE dim.id_client IS NULL;
    
   
    drop table tmp_clients;
   
   
END;




$$
;
