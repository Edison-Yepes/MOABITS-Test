create table dwh.dim_clients(
	  id_client integer
	, cod_client varchar(50)
	, name_client varchar(50)
	, load_date timestamp
	, updated_at timestamp
	, primary key (id_client)
);


create table dwh.dim_types(
	  id_type integer
	, cod_type varchar(50)
	, desc_type varchar(50)
	, load_date timestamp
	, updated_at timestamp
	, primary key (id_type)
);


create table dwh.dim_countries(
	  id_country integer
	, cod_iso3_country varchar(50)
	, cod_mcc_country varchar(50)
	, country_name varchar(50)
	, load_date timestamp
	, updated_at timestamp
	, primary key (id_country)
);


create table dwh.dim_networks_subscriptors(
	  id_network_subscriptor integer
	, cod_mnc_network varchar(50)
	, cod_imsi_network_subscriptor varchar(50)
	, load_date timestamp
	, updated_at timestamp
	, primary key (id_network_subscriptor)
);



create table dwh.fact_transactions (
    id_transaction integer
  , id_type integer
  , id_client integer
  , id_country integer
  , id_network_subscriptor integer
  , connect_time timestamp
  , close_time timestamp
  , consumption integer
  , load_date timestamp
  , updated_at timestamp 
  ,  primary key (id_transaction)
);


create table dwh.audit(
	  ICCID varchar(50)
	, description varchar(50)
)
;

