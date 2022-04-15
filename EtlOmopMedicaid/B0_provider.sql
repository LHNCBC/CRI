-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','Provider','1','start provider',current_timestamp(), null);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");

-- COMMAND ----------

drop table if exists dua_052538_nwi388.hold_provider2;

  create table
  dua_052538_nwi388.hold_provider2 as
select
  concat(clm_id, state_cd, 32855) as forign_key,              
  concat(state_cd, clm_id, "inpatient_header") as primary_key,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "inpatient_header" as origin_file
from
dua_052538_nwi388.inpatient_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','2','create hold provider',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_provider2
select

  concat(clm_id, state_cd, 32855) as forign_key,
  concat(state_cd, clm_id, "inpatient_line") as primary_key,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "inpatient_line" as origin_file
from
dua_052538_nwi388.inpatient_line_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','3','inpt line to hold provider',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_provider2
select
  concat(clm_id, state_cd, 32861) as forign_key, 
  concat(state_cd, clm_id, "other_services_header") as primary_key,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "other_services_header" as origin_file
from
dua_052538_nwi388.other_services_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','4','ot header to hold provider',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_provider2
select

  concat(clm_id, state_cd, 32861) as forign_key,
  concat(state_cd, clm_id, "other_services_line") as primary_key,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "other_services_line" as origin_file
from
dua_052538_nwi388.other_services_line_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','4','ot line to hold provider',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_provider2
select
  
  concat(clm_id,state_cd, 32846) as forign_key,--Facility claim header
  concat(state_cd, clm_id, "long_term_header") as primary_key,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "long_term_header" as origin_file
from
dua_052538_nwi388.long_term_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','5','lt header to hold provider',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_provider2
select
  
  concat(clm_id, state_cd, 32846) as forign_key,--Facility claim header
  concat(state_cd, clm_id, "long_term_line") as primary_key,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "long_term_line" as origin_file
from
dua_052538_nwi388.long_term_line_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','6','lt line to hold provider',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_provider2
select

  concat(clm_id, state_cd, 32869) as forign_key,--Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as primary_key,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "rx_header" as origin_file
from
dua_052538_nwi388.rx_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','7','rx header 1 to hold provider',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_provider2
select

  concat(clm_id, state_cd, 32869) as forign_key,--Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as primary_key,
  PRSCRBNG_PRVDR_NPI as npi,
  null as specialty_source_value,
  "PRSCRBING" as class,
  "rx_header" as origin_file
from
dua_052538_nwi388.rx_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','8','rx header 2 to hold provider',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_provider2
select

  concat(clm_id, state_cd, 32869) as forign_key,--Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as primary_key,
  DSPNSNG_PRVDR_NPI as npi,
  null as specialty_source_value,
  "DSPNSNG" as class,
  "rx_header" as origin_file
from
dua_052538_nwi388.rx_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','9','rx header 3 to hold provider',current_timestamp(), null);

-- COMMAND ----------

----optimize  dua_052538_nwi388.hold_provider2 ZORDER by(npi);

-- COMMAND ----------


drop table if exists dua_052538_nwi388.transform_provider;
create table  dua_052538_nwi388.transform_provider using delta as
select
  distinct npi
from
  dua_052538_nwi388.hold_provider2;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','11','create transform provider',current_timestamp(), null);

-- COMMAND ----------

--optimize  dua_052538_nwi388.transform_provider ZORDER by(npi);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','12','optimize transform provider',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.transform_provider_details;
create table dua_052538_nwi388.transform_provider_details using delta as
select
  a.npi,
  Entity_type_code,
  provider_organization_name_legal_business_name,
  provider_last_name_legal_name,
  provider_first_name,
  provider_name_prefix_text,
  healthcare_provider_taxonomy_group_1
from
  dua_052538_nwi388.transform_provider a
  left join dua_052538_nwi388.NPI_provider b on a.npi = b.npi;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','13','create transform provider details',current_timestamp(), null);

-- COMMAND ----------

  drop table if exists dua_052538_nwi388.transform_provider_specialty;
create table dua_052538_nwi388.transform_provider_specialty using delta as
select
  distinct npi as npi_b,
  specialty_source_value
from
  dua_052538_nwi388.hold_provider2
where
  npi is not null
  and specialty_source_value is not null
  ;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','14','create transform provider specialty',current_timestamp(), null);

-- COMMAND ----------

--optimize  dua_052538_nwi388.transform_provider_specialty ZORDER by(npi_b);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','15','optimize transform provider specialty',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.provider_detail_a;
create table dua_052538_nwi388.provider_detail_a using delta as
select
  concat(
    provider_first_name,
    "_",
    provider_last_name_legal_name
  ) as provider_name,
  npi as npi,
  npi as provider_source_value
from
  dua_052538_nwi388.transform_provider_details
where
  npi != 'NA'
  and Entity_Type_Code = 1;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','16','create provider detail a',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.provider_detail_b;
create table dua_052538_nwi388.provider_detail_b using delta as
select
  provider_organization_name_legal_business_name as provider_name,
  npi,
  npi as provider_source_value
from
  dua_052538_nwi388.transform_provider_details
where
  npi != 'NA'
  and Entity_Type_Code = 2;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','17','create provider detail b',current_timestamp(), null);

-- COMMAND ----------

drop view if exists provider_detail;
create view provider_detail as
select
  *
from
  dua_052538_nwi388.provider_detail_a
union
select
  *
from
  dua_052538_nwi388.provider_detail_b;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','18','create view provider detail',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.provider_detail_all;
create table dua_052538_nwi388.provider_detail_all using delta as
select
  npi as provider_id,
  provider_name,
  npi,
  provider_source_value
from
  provider_detail;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','19','create provider detail all',current_timestamp(), null);

-- COMMAND ----------

--optimize  dua_052538_nwi388.provider_detail_all zorder by(npi);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','20','potimize provider detail all',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.provider
select
  npi,
  npi as provider_id,
  provider_name,
  null as dea,
  null as pecialty_concept_id,
  null as care_site_id,
  null as year_of_birth,
  null as gender_concept_id,
  null as provider_source_value,
  a.specialty_source_value,
  null as specialty_sourse_concept_id,
  null as gender_source_value,
  null as gender_source_concept_id
from  
dua_052538_nwi388.transform_provider_specialty a
 
 left join
 dua_052538_nwi388.provider_detail_all b
 on a.npi_b = b.npi;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','21','transform provider and detail to Cprovider DM',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Provider','22','provider end',current_timestamp(), null);