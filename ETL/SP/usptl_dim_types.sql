CREATE OR REPLACE PROCEDURE dwh.usptl_dim_types()
	LANGUAGE plpgsql
AS $$
	
	
	
BEGIN

	--identify unic clients
	CREATE TEMP TABLE tmp_types AS 
	SELECT DISTINCT 
		 "TYPE"
	FROM stg.cliente_raw_data;
	
	CREATE TEMP TABLE stage(LIKE dwh.dim_types);
	
	INSERT INTO stage
	SELECT
		  dim.id_type
		, stg."TYPE" AS cod_type
		, stg."TYPE" AS desc_type
	FROM tmp_types AS stg
    INNER JOIN dwh.dim_types AS dim 
    	ON dim.cod_type = stg."TYPE";

    
  	--insert old rows (UPDATE)	
	DELETE FROM dwh.dim_types
  	USING stage
  	WHERE dwh.dim_types.id_type = stage.id_type;

    INSERT INTO dwh.dim_types  
    (
		  id_type
		, cod_type
		, desc_type
		, load_date
		, updated_at
	)
 	SELECT 		 
 		  id_type
 		, cod_type
 		, desc_type
 		, current_timestamp
 		, current_timestamp
	FROM stage;

	drop table stage;
 

    
   --insert new rows
 	INSERT INTO dwh.dim_types
	(
		  id_type
		, cod_type
		, desc_type
		, load_date
		, updated_at
	)
	SELECT
		  CAST(COALESCE(dim.id_type, SUM(NVL2(dim.id_type, 0, 1)) OVER(ORDER BY stg."TYPE" ROWS UNBOUNDED PRECEDING) + MAXID.ID) AS INTEGER ) AS id_type
		, stg."TYPE" AS cod_type
		, stg."TYPE" AS desc_type
		, current_timestamp
		, current_timestamp
	FROM tmp_types AS stg
	--JOIN ESTRUCTURA MAX_IDS
	CROSS JOIN ( SELECT COALESCE(MAX(id_type),0) AS ID FROM dwh.dim_types) MAXID--autogenerado
    LEFT JOIN dwh.dim_types AS dim 
    	ON dim.cod_type = stg."TYPE"
    WHERE dim.id_type IS NULL;
    
   
    drop table tmp_types;
   
   
END;




$$
;
