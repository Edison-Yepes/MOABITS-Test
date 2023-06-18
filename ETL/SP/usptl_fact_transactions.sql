CREATE OR REPLACE PROCEDURE dwh.usptl_fact_transactions()
	LANGUAGE plpgsql
AS $$
	
	
	
	
BEGIN

	--Identify staging rows
	CREATE TEMP TABLE stage AS
	SELECT
		  dim.id_transaction
		, stg.CDR_ID AS cod_transaction
		, stg.ICCID AS cod_ICCID
		, COALESCE(types.id_type, -1) AS id_type
		, stg."type" AS cod_type
		, COALESCE(cli.id_client, -1) AS id_client
		, stg.INVENTORY_ID AS cod_client
		, COALESCE(cou.id_country, -1) AS id_country
		, stg.country_iso3 AS cod_country
		, COALESCE(nwts.id_network_subscriptor, -1) AS id_network_subscriptor
		, stg.mnc AS cod_mnc_network
		, stg.imsi_id AS cod_imsi_network_subscriptor
		, dim.connect_time
		, dim.close_time
		, dim.consumption
	FROM stg.cliente_raw_data AS stg
    INNER JOIN dwh.fact_transactions AS dim 
    	ON stg.CDR_ID = dim.cod_transaction
    LEFT JOIN dwh.dim_clients cli
   		ON stg.INVENTORY_ID = cli.cod_client
   	LEFT JOIN dwh.dim_types  types
   		ON stg."TYPE" = types.cod_type 
   	LEFT JOIN dwh.dim_countries cou
   		ON stg.country_iso3 = cou.cod_iso3_country
	LEFT JOIN dwh.dim_networks_subscriptors nwts
   		ON stg.mnc = nwts.cod_mnc_network
   		AND stg.imsi_id = nwts.cod_imsi_network_subscriptor;

	
	DELETE FROM dwh.fact_transactions
  	USING stage
  	WHERE dwh.fact_transactions.id_transaction = stage.id_transaction;

  	--insert old rows (UPDATE)
    INSERT INTO dwh.fact_transactions  
    (
		  id_transaction
		, cod_transaction
		, cod_ICCID
		, id_type
		, id_client
		, id_country
		, id_network_subscriptor
		, connect_time
		, close_time
		, consumption
		, load_date
		, updated_at
	)
 	SELECT 		 
 		  id_transaction
 		, cod_transaction
 		, cod_ICCID
		, id_type
		, id_client
		, id_country
		, id_network_subscriptor
		, connect_time
		, close_time
		, consumption
		, current_timestamp
 		, current_timestamp
	FROM stage;

	drop table stage;
 

    
   --insert new rows
 	INSERT INTO dwh.fact_transactions
	(
		  id_transaction
		, cod_transaction
		, cod_ICCID
		, id_type
		, id_client
		, id_country
		, id_network_subscriptor
		, connect_time
		, close_time
		, consumption
		, load_date
		, updated_at
	)
	SELECT
		CAST(COALESCE(dim.id_transaction, SUM(NVL2(dim.id_transaction, 0, 1)) OVER(ORDER BY stg.CDR_ID ROWS UNBOUNDED PRECEDING) + MAXID.ID) AS INTEGER ) AS id_transaction
		, stg.CDR_ID
		, stg.ICCID
		, COALESCE(types.id_type, -1) AS id_type
		, COALESCE(cli.id_client, -1) AS id_client
		, COALESCE(cou.id_country, -1) AS id_country
		, COALESCE(nwts.id_network_subscriptor, -1) AS id_network_subscriptor
		, CAST(stg.connect_time AS TIMESTAMP) connect_time
		, CAST(stg.close_time AS TIMESTAMP) close_time
		, CAST(stg.duration AS INTEGER) AS consumption
	FROM stg.cliente_raw_data AS stg
	--JOIN ESTRUCTURA MAX_IDS
	CROSS JOIN ( SELECT COALESCE(MAX(id_transaction),0) AS ID FROM dwh.fact_transactions) MAXID--autogenerado
    LEFT JOIN dwh.fact_transactions AS dim 
    	ON stg.CDR_ID = dim.cod_transaction
    LEFT JOIN dwh.dim_clients cli
   		ON stg.INVENTORY_ID = cli.cod_client
   	LEFT JOIN dwh.dim_types  types
   		ON stg."TYPE" = types.cod_type 
   	LEFT JOIN dwh.dim_countries cou
   		ON stg.country_iso3 = cou.cod_iso3_country
	LEFT JOIN dwh.dim_networks_subscriptors nwts
   		ON stg.mnc = nwts.cod_mnc_network
   		AND stg.imsi_id = nwts.cod_imsi_network_subscriptor
    WHERE dim.id_transaction IS NULL;
    
    
END;









$$
;
