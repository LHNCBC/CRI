-- Databricks notebook source
--this file takes 'Load' data and transforms it into the destination data
--Fragments are numbered by their command-
--fragments are processed in order for a reason
--fragments are processed at the smallest unit avaialble for a reason 
--view to table to view to insert transactions are intetional

-- COMMAND ----------

--begin provider transformations

-- COMMAND ----------

drop table if exists dua_052538_nwi388.transform_provider;
create table dua_052538_nwi388.transform_provider(
complex_id string,
npi string,
specialty_source_value string,
class string,
original_file string
);

-- COMMAND ----------

--inpt head
insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"inpatient_header")as complex_id,
  RFRG_PRVDR_NPI as npi, 
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "inpatient_header" as original_file
  from inpatient_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"inpatient_header")as complex_id,
  ADMTG_PRVDR_NPI as npi, 
  ADMTG_PRVDR_SPCLTY_CD as specialty_source_value,
  "ADMTG" as class,
  "inpatient_header" as original_file
  from inpatient_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"inpatient_header")as complex_id,
  BLG_PRVDR_NPI as npi, 
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "inpatient_header" as original_file
  from inpatient_header;
  
insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"inpatient_line")as complex_id,
  SRVC_PRVDR_NPI as npi, 
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "inpatient_line" as original_file
  from inpatient_line;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"inpatient_line")as complex_id,
  OPRTG_PRVDR_NPI as npi,
  "NULL" as specialty_source_value,
  "OPRTG" as class,
  "inpatient_line" as original_file
  from inpatient_line
  --ot head

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"other_services_header")as complex_id,
  BLG_PRVDR_NPI as npi, 
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "other_services_header" as original_file
  from other_services_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"other_services_header")as complex_id,
  RFRG_PRVDR_NPI as npi, 
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "other_services_header" as original_file
  from other_services_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"other_services_header")as complex_id,
  drctng_prvdr_npi as npi, 
  "NULL"as specialty_source_value,
  "DRCTNG" as class,
  "other_services_header" as original_file
  from other_services_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"other_services_header")as complex_id,
  sprvsng_prvdr_npi as npi,
  "NULL"as specialty_source_value,
  "SPRVSNG" as class,
  "other_services_header" as original_file
  from other_services_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"other_services_header")as complex_id,
  hlth_home_prvdr_npi as npi, 
  "NULL"as specialty_source_value,
  "HLTH_HOME" as class,
  "other_services_header" as original_file
  from other_services_header;
  

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"other_services_line")as complex_id,
  SRVC_PRVDR_NPI as npi, 
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "other_services_line" as original_file
  from other_services_line;
--lt header
  
insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"long_term_header")as complex_id,
  RFRG_PRVDR_NPI as npi, 
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "long_term_header" as original_file
  from long_term_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"long_term_header")as complex_id,
  ADMTG_PRVDR_NPI as npi,
  ADMTG_PRVDR_SPCLTY_CD as specialty_source_value,
  "ADMTG" as class,
  "long_term_header" as original_file
  from long_term_header;
-- lt line

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"long_term_header")as complex_id,
  BLG_PRVDR_NPI as npi, 
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "long_term_header" as original_file
  from long_term_header;
  
  insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"long_term_line")as complex_id,
  SRVC_PRVDR_NPI as npi, 
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "long_term_line" as original_file
  from long_term_line;

--rx head

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"rx_header")as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "rx_header" as original_file
  from rx_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"rx_header")as complex_id,
  PRSCRBNG_PRVDR_NPI as npi, 
  "NULL" as specialty_source_value,
  "PRSCRBING" as class,
  "rx_header" as original_file
  from rx_header;

insert into dua_052538_nwi388.transform_provider select
  concat(state_cd,clm_id,"rx_header")as complex_id,
  DSPNSNG_PRVDR_NPI as npi, 
  "NULL" as specialty_source_value,
  "DSPNSNG" as class,
  "rx_header" as original_file
  from rx_header;

-- COMMAND ----------

-- there may be a better way to handle this-
-- which provider types go in the provider table should be decided here!!!
drop view if exists provider_table_route;
create view provider_table_route as
select 
distinct complex_id,
a.npi,
specialty_source_value,
class,
Entity_type_code,
provider_organization_name_legal_business_name,
provider_last_name_legal_name,
provider_first_name,
provider_name_prefix_text, 
healthcare_provider_taxonomy_group_1,
original_file
from 
dua_052538_nwi388.transpose_provider a
left join
NPI_provider b
on
a.npi=b.npi
where a.npi is not null
and
class='BLG' or class='SRVC';

-- COMMAND ----------

--here the data is loaded where and only where kinds of NPI records are needed-
--this may change as facility level providers may not go in provider table
drop table if exists dua_052538_nwi388.provider_for_provider_table;
create table dua_052538_nwi388.provider_for_provider_table (
  provider_name string, 
  npi string , 
  dea string , 
  specialty_concept_id string , 
  care_site_id string , 
  year_of_birth string , 
  gender_concept_id string , 
  provider_source_value string , 
  specialty_source_value string , 
  specialty_source_concept_id string , 
  gender_source_value string , 
  gender_source_concept_id string,
  class string,
  complex_id string);

-- COMMAND ----------

--care site is currently unresolved;
--it should come from visit location table as providers should be one record per NPI, not per npi-per-locaiton
insert into dua_052538_nwi388.provider_for_provider_table select
  concat(provider_first_name,"_",provider_last_name_legal_name) as provider_name, 
  npi, 
  'null'as dea, --we will never have this
  'null' as specialty_concept_id, --requires ccw dicitonary
  'null' as care_site_id, --requires location table
  'null' as year_of_birth,  --we will never have this
  'null' as gender_concept_id,  --we will never have this
   npi as provider_source_value, 
  specialty_source_value, 
  'null' as specialty_source_concept_id,  --requires ccw dicitonary
  'null' as gender_source_value, --we will never have this
  'null' as gender_source_concept_id,--we will never have this
  class,
  complex_id
  from provider_table
  where npi !='NA' and class='SRVC' and provider_last_name_legal_name !='NA';
  
  
  insert into dua_052538_nwi388.provider_for_provider_table select
  concat(provider_first_name,"_",provider_last_name_legal_name) as provider_name, 
  npi, 
  'null'as dea, 
  'null' as specialty_concept_id, 
  'null' as care_site_id, 
  'null' as year_of_birth, 
  'null' as gender_concept_id, 
   npi as provider_source_value, 
  specialty_source_value, 
  'null' as specialty_source_concept_id, 
  'null' as gender_source_value, 
  'null' as gender_source_concept_id,
  class,
  complex_id
  from provider_table
  where npi !='NA' and class='BLG' and provider_last_name_legal_name !='NA';
  
  insert into dua_052538_nwi388.provider_for_provider_table select
  provider_organization_name_legal_business_name as provider_name, 
  npi, 
  'null'as dea, 
  'null' as specialty_concept_id, 
  'null' as care_site_id, 
  'null' as year_of_birth, 
  'null' as gender_concept_id, 
   npi as provider_source_value, 
  specialty_source_value, 
  'null' as specialty_source_concept_id, 
  'null' as gender_source_value, 
  'null' as gender_source_concept_id,
  class,
  complex_id
  from provider_table
  where npi !='NA' and class='BLG' and provider_last_name_legal_name !='NA';
  
   insert into dua_052538_nwi388.provider_routes select
  provider_organization_name_legal_business_name as provider_name, 
  npi, 
  'null'as dea, 
  'null' as specialty_concept_id, 
  'null' as care_site_id, 
  'null' as year_of_birth, 
  'null' as gender_concept_id, 
   npi as provider_source_value, 
  specialty_source_value, 
  'null' as specialty_source_concept_id, 
  'null' as gender_source_value, 
  'null' as gender_source_concept_id,
  class,
  complex_id
  from provider_table
  where npi !='NA' and class='BLG' and provider_last_name_legal_name !='NA';

-- COMMAND ----------

--load provider into provider x
insert into dua_052538_nwi388.provider_x select
distinct npi,
provider_name,
dea, 
specialty_concept_id, 
care_site_id, 
year_of_birth, 
gender_concept_id, 
provider_source_value, 
specialty_source_value, 
specialty_source_concept_id, 
gender_source_value, 
gender_source_concept_id,
ROW_NUMBER() OVER(ORDER BY npi ASC)  as proivder_id
from dua_052538_nwi388.provider_routes;


-- COMMAND ----------

--end provider transform

-- COMMAND ----------

--start visit occourrence transform
--consider making a non-native claim id- 

-- COMMAND ----------

drop table if exists dua_052538_nwi388.transform_visit_occ;
create table dua_052538_nwi388.transform_visit_occ
(
visit_occurrence_id string,
person_id bigint,
visit_source_value string,
admitting_source_value string,
visit_start_date date,
visit_start_datetime string,
visit_end_date date,
visit_end_datetime string,
original_table string
);  

-- COMMAND ----------

--inpatient_header

insert into dua_052538_nwi388.transform_visit_occ  select
  concat(state_cd,clm_id,"inpatient_header")  as visit_occurrence_id,
  bene_id as person_id,
  HOSP_TYPE_CD as visit_source_value,
  ADMSN_TYPE_CD as admitting_source_value,
  ADMSN_DT as visit_start_date,
  ADMSN_HR as visit_start_datetime,
  DSCHRG_DT as visit_end_date,
  DSCHRG_HR as visit_end_datetime,
  "inpatient_header" as original_table
  from inpatient_header; 
  
  --inpatient_line
insert into dua_052538_nwi388.transform_visit_occ  select
  concat(state_cd,clm_id,"inpatient_line")  as visit_occurrence_id,
  bene_id as person_id,
  TOS_CD as visit_source_value,
  "NULL" as admitting_source_value,
  LINE_SRVC_BGN_DT as visit_start_date,
  "NULL" as visit_start_datetime,
  LINE_SRVC_END_DT as visit_end_date,
  "NULL" as visit_end_datetime,
  "inpatient_line" as original_table
  from inpatient_line; 
  
  --long_term_header
insert into dua_052538_nwi388.transform_visit_occ  select
  concat(state_cd,clm_id,"long_term_header")  as visit_occurrence_id,
  bene_id as person_id,
  "NULL" as visit_source_value,
  "NULL" as admitting_source_value,
  ADMSN_DT as visit_start_date,
  ADMSN_HR as visit_start_datetime,
  DSCHRG_DT as visit_end_date,
  DSCHRG_HR as visit_end_datetime,
  "long_term_header" as original_table
  from long_term_header; 
  
  --long_term_line
insert into dua_052538_nwi388.transform_visit_occ  select
  concat(state_cd,clm_id,"long_term_line")  as visit_occurrence_id,
  bene_id as person_id,
  TOS_CD as visit_source_value,
  "NULL" as admitting_source_value,
  LINE_SRVC_BGN_DT as visit_start_date,
  "NULL" as visit_start_datetime,
  LINE_SRVC_END_DT as visit_end_date,
  "NULL" as visit_end_datetime,
  "long_term_line" as original_table
  from long_term_line; 
  
  --other_services_header
insert into dua_052538_nwi388.transform_visit_occ select
  concat(state_cd,clm_id,"other_services_header")  as visit_occurrence_id,
  bene_id as person_id,
  POS_CD as visit_source_value,
  "NULL" as admitting_source_value,
  srvc_bgn_dt as visit_start_date,
  "NULL" as visit_start_datetime,
  srvc_end_dt as visit_end_date,
  "NULL" as visit_end_datetime,
  "other_services_header"as original_table
  from other_services_header; 
  
    --other_services_line   
insert into dua_052538_nwi388.transform_visit_occ  select
  concat(state_cd,clm_id,"other_services_line")  as visit_occurrence_id,
  bene_id as person_id,
  TOS_CD as visit_source_value,
  "NULL" as admitting_source_value,
  LINE_SRVC_BGN_DT as visit_start_date,
  "NULL" as visit_start_datetime,
  LINE_SRVC_END_DT  as visit_end_date,
  "NULL" as visit_end_datetime,
  "other_services_line"as original_table
  from other_services_line; 
  --medications do not count as visit at this time

-- COMMAND ----------

insert into dua_052538_nwi388.VISIT_OCCURRENCE_x select
visit_occurrence_id, 
person_id, 
0 as visit_concept_id , --this needs to be recoded as athena vocab for claims type?
visit_start_date, 
visit_start_datetime, 
visit_end_date date, 
visit_end_datetime, 
0 as visit_type_concept_id, 
0 as provider_id, --this should reflect visit npi under visit_occurrence id join to provider_x
0 care_site_id, --requires ccw dictionary
visit_source_value, 
0 as visit_source_concept_id, --requires ccw dictionary
'null' as admitting_source_concept_id , --need a method to handle this
admitting_source_value, -- this may be a bad map
'NULL' as discharge_to_concept_id, --this can come from a preprocessing step above
'NULL' AS discharge_to_source_value, --this can come from a preprocessing step above
'NULL' AS preceding_visit_occurrence_id --this can come from a preprocessing step above
from dua_052538_nwi388.transform_visit_occ;

-- COMMAND ----------

--end transform visit occurrence

-- COMMAND ----------

--begin transform DX/condition occourrence

-- COMMAND ----------

drop table if exists dua_052538_nwi388.transform_dx;
create table dua_052538_nwi388.transform_dx
(
clm_id bigint,
original_table string,
original string,
event_source_value string,
event_start_date date,
event_end_date date,
complex_id string--,
--concept_name string,
--concept_id bigint,
--domain string,
--vocabulary_id bigint
);

-- COMMAND ----------

--OT header
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "other_services_header" as original_table,
  state_cd as original,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"other_services_header") as complex_id
  from other_services_header
 ;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "other_services_header" as original_table,
  state_cd as original,
  DGNS_CD_2 as event_source_value,
 SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"other_services_header") as complex_id
  from other_services_header;
  
  
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  ADMTG_DGNS_CD as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id 
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_3 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id 
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_4 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id 
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_5 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id 
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_6 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_7 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id 
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_8 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_9 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_10 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_11 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  DGNS_CD_12 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  drg_cd as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
  --LT Header

insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "long_term_header" as original_table,
  state_cd as original,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"long_term_header") as complex_id 
  from long_term_header;

insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "long_term_header" as original_table,
  state_cd as original,
  DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"long_term_header") as complex_id  
  from long_term_header;

insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "long_term_header" as original_table,
  state_cd as original,
  DGNS_CD_3 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"long_term_header") as complex_id  
  from long_term_header;

insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "long_term_header" as original_table,
  state_cd as original,
  DGNS_CD_4 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"long_term_header") as complex_id  
  from long_term_header;

insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "long_term_header" as original_table,
  state_cd as original,
  DGNS_CD_5 as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"long_term_header") as complex_id 
  from long_term_header;

insert into dua_052538_nwi388.transform_dx select 
  clm_id,
  "long_term_header" as original_table,
  state_cd as original,
  ADMTG_DGNS_CD as event_source_value,
  SRVC_BGN_DT as event_start_date ,
  SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"long_term_header") as complex_id  
  from long_term_header;
  
  

-- COMMAND ----------

drop view if exists dx_routes;
create view dx_routes as 
select
clm_id,
original_table,
original,
event_source_value,
event_start_date,
event_end_date,
complex_id,
concept_name,
concept_id,
domain_id,
vocabulary_id
from dua_052538_nwi388.transform_dx a
left join lkup_ICD10CM b
on
a.event_source_value=b.concept_code
where event_source_value !='';

-- COMMAND ----------

--the above needs to be tested before fed to condition_occourrence_x

-- COMMAND ----------

--conditon occourrence transform ends

-- COMMAND ----------

--procedure occourrence transform starts

-- COMMAND ----------

drop table if exists dua_052538_nwi388.transform_px;
create table dua_052538_nwi388.transform_px
(clm_id bigint,
original_table string,
original string,
event_source_value string,
event_start_date date,
complex_id string,
concept_name string,
concept_id string,
domain_id string,
vocabulary_id string
);

-- COMMAND ----------

--in this step, take procedure to dicitonary join and do it in one step

--OT Line

insert into dua_052538_nwi388.transform_px select 
  clm_id,
  "other_services_line" as original_table,
  state_cd as original,
  LINE_PRCDR_CD as event_source_value,
  --LINE_PRCDR_MDFR_CD_1,
  --LINE_PRCDR_MDFR_CD_2,
  --LINE_PRCDR_MDFR_CD_3,
  --LINE_PRCDR_MDFR_CD_4,
  --TOOTH_DSGNTN_SYS,
  --TOOTH_NUM,
  --TOOTH_ORAL_CVTY_AREA_DSGNTD_CD,
  --TOOTH_SRFC_CD,
  LINE_PRCDR_CD_DT as event_start_date,
  concat(state_cd,clm_id,"other_services_line") as complex_id,
  from other_services_line;
  
  --IP header
insert into dua_052538_nwi388.transform_px select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  prcdr_cd_1 as event_source_value,
  prcdr_cd_dt_1 as event_start_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
  
insert into dua_052538_nwi388.transform_px select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  prcdr_cd_2 as event_source_value,
  prcdr_cd_dt_2 as event_start_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
 
  
insert into dua_052538_nwi388.transform_px select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  prcdr_cd_3 as event_source_value,
  prcdr_cd_dt_3 as event_start_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;
  
insert into dua_052538_nwi388.transform_px select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  prcdr_cd_4 as event_source_value,
  prcdr_cd_dt_4 as event_start_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header;

  
 insert into dua_052538_nwi388.transform_px select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  prcdr_cd_5 as event_source_value,
  prcdr_cd_dt_5 as event_start_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header ;
  
  
insert into dua_052538_nwi388.transform_px select 
  clm_id,
  "inpatient_header" as original_table,
  state_cd as original,
  prcdr_cd_6 as event_source_value,
  prcdr_cd_dt_6 as event_start_date,
  concat(state_cd,clm_id,"inpatient_header") as complex_id
  from inpatient_header  
 ;

-- COMMAND ----------

drop view if exists transform_px_visit_provider_athena;
create view transform_px_visit as
select * from 
dua_052538_nwi388.transform_px a
left join     
dua_052538_nwi388.VISIT_OCCURRENCE_x b--visit joing
on
a.complex_id=b.visit_occurrence_id
left join 
lkup_ICD10CMPCS
on  a.event_source_value=c.concept_code--athena join
--need a provider join too!
;

-- COMMAND ----------

insert into dua_052538_NWI388.procedure_occurrence_x select
ROW_NUMBER() OVER(ORDER BY person_id,clm_id,original,original_table ASC) as procedure_occurrence_id,
person_id,
concept_id as procedure_concept_id,--no work has been done to keep this value standard 
visit_start_date as procedure_date,
visit_start_datetime as procedure_datetime,
0 as procedure_type_concept_id,
0 as modifier_concept_id,--not currently useing this
0 as quantity,
0 as provider_id,
visit_occurrence_id,
visit_occurrence_id as visit_detail_id,--this needs to be mapped- this is the original value
event_source_value as procedure_source_value,
concept_id as procedure_source_concept_id,--this is a concept ID that can be standard or non
0 as modifier_source_value	--not currently useing this
from transform_px_visit_provider_athena;

-- COMMAND ----------

--procedure occourrence transform ends

-- COMMAND ----------

--medication exposure transformation begins

-- COMMAND ----------

drop table if exists dua_052538_nwi388.transform_rx;
create table dua_052538_nwi388.transform_rx
(
clm_id bigint,
original_table string,
original string,
event_source_value string,
dose_unit_source_value string,
quantity integer,
refills string,
days_supply integer,
route_source_value string,
event_start_date date,
event_end_date date

);

-- COMMAND ----------

--ot_line
insert into dua_052538_nwi388.transform_rx select 
  clm_id,
  "other_services_line" as original_table,
  state_cd as original,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(a.state_cd,a.clm_id,"other_services_line") as complex_id
  from other_services_line;
  
  --ip line
insert into dua_052538_nwi388.transform_rx select 
  clm_id,
  "inpatient_line" as original_table,
  state_cd as original,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"inpatient_line") as complex_id
  from inpatient_line ;
  --rx line
insert into dua_052538_nwi388.transform_rx select 
  clm_id,
  "rx_line" as original_table,
  state_cd as original,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  NEW_RX_REFILL_NUM as refills,
  DAYS_SUPPLY as days_supply,
  DOSAGE_FORM_CD as route_source_value,
  RX_FILL_DT as event_start_date,
  date('01/01/0001') as event_end_date,
  concat(state_cd,clm_id,"rx_line") as complex_id
  from rx_line;
  
  --lt line
insert into dua_052538_nwi388.transform_rx select 
  clm_id,
  "long_term_line" as original_table,
  state_cd as original,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  0 as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(state_cd,clm_id,"long_term_line") as complex_id 
  from long_term_line;

-- COMMAND ----------

drop view if exists medicaiton_route;
create view medication_route as select * from
dua_052538_nwi388.transform_rx
left join
lkup_ndc on ndc=concept_code;
-- should this have a visit id?

-- COMMAND ----------

--insert into medicaiton_exposure_x goes here

-- COMMAND ----------

--medication exposure transformation ends

-- COMMAND ----------

--observation period transformation begins

-- COMMAND ----------

insert into dua_052538_nwi388.OBSERVATION_PERIOD_x select
ROW_NUMBER() OVER(ORDER BY bene_id ASC) AS observation_period_id,
bene_id as person_id,
enrlmt_start_dt as observation_period_start_date,
enrlmt_end_dt as  observation_period_end_date,
"NULL" as period_type_concept_id,
state_cd
from
demog_elig;

-- COMMAND ----------

--observation period transformation ends

-- COMMAND ----------

--person table transformation begins
--this method only works on one year of data; 

-- COMMAND ----------

drop view if exists transpose_person;
create view transpose_person as select
BENE_ID,
case 
when SEX_CD='F' then 8532
when SEX_CD='M' then 8507
else 0 end as gender_concept_id,
year(BIRTH_DT) as year_of_birth,
month(BIRTH_DT) as month_of_birth,
day(BIRTH_DT) as day_of_birth,
case 
when RACE_ETHNCTY_CD=1 then 8527
when RACE_ETHNCTY_CD=2 then 8516
when RACE_ETHNCTY_CD=3 then 8515
when RACE_ETHNCTY_CD=4 then 8657
when RACE_ETHNCTY_CD=5 then 8557
when RACE_ETHNCTY_CD=6 then 8522
when RACE_ETHNCTY_CD=7 then 38003563
else 0 end as race_concept_id,
case 
when ETHNCTY_CD=0 then 38003564
when ETHNCTY_CD>0 then 38003563
else 0 end as ethnicity_concept_id,
bene_id as person_source_value,
SEX_CD as gender_source_value ,
RACE_ETHNCTY_CD as race_source_value,
ETHNCTY_CD as ethnicity_source_value,
STATE_CD
from demog_elig_base
;

-- COMMAND ----------

insert into dua_052538_NWI388.PERSON_x select
BENE_ID as person_id,
 gender_concept_id,
year_of_birth,
month_of_birth,
 day_of_birth,
0 as birth_datetime, 
race_concept_id, 
ethnicity_concept_id,
0 as location_id,
0 as provider_id,
0 as care_site_id,
bene_id as person_source_value,
gender_source_value ,
"NULL" as gender_source_concept_id,
race_source_value,
"NULL" as race_source_concept_id,
ethnicity_source_value,
"NULL" as ethnicity_source_concept_id
from transpose_person;

-- COMMAND ----------

--person table transformation ends
