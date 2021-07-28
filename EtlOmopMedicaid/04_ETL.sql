-- Databricks notebook source
--high level to do's
--where to preserve state_cd as origin?
--where to preserve dx-px order?
--where to resolve events that have no end date?

--four major asks
--1 multi year support
--2 de dup support
--3 better visits
--4 beter providers


-- COMMAND ----------

--high level guide

--the following program contains (up to) three steps per destination table
--within the three steps there are ordered fragments which achieve the steps

--the steps are
--hold
--transform
--route

--hold collects and holds data required for the destination table
--transform transforms all data from hold into a destination format
--route readies data to be inserted into a destination table

-- COMMAND ----------

--table
--person

--destination table: person
--step number: 1
--step name: hold

--this step is necessary for multi-year person records

-- COMMAND ----------

--destination table: person
--step number: 2
--step name: transform

-- multi level person records are deduplicated here

-- COMMAND ----------

--hardcode

--destination table: person
--step number: 3
--step name: route

--fragment #1
--fragment comment:extract person values for 05_destination

 -- Error in SQL statement: SparkException: Cannot recognize hive type string: null, column: birth_datetime


drop table if exists dua_052538_nwi388.route_person;
create table dua_052538_nwi388.route_person(
  person_id bigint,
  gender_concept_id bigint,
  year_of_birth bigint,
  month_of_birth bigint,
  day_of_birth bigint,
  birth_datetime string,
  race_concept_id bigint,
  ethnicity_concept_id bigint,
  location_id bigint,
  provider_id bigint,
  care_site_id bigint,
  person_source_value string,
  gender_source_value string,
  gender_source_concept_id bigint,
  race_source_value string,
  race_source_concept_id bigint,
  ethnicity_source_value string,
  ethnicity_source_concept_id bigint
); 

drop view if exists route_person;
create view route_person as
select
  BENE_ID,
  case
    when SEX_CD = 'F' then 8532--Female|Female
    when SEX_CD = 'M' then 8507--Male|Male
    else 0--|blank
  end as gender_concept_id,
  year(BIRTH_DT) as year_of_birth,
  month(BIRTH_DT) as month_of_birth,
  day(BIRTH_DT) as day_of_birth,
 -- null as birth_datetime,
  case
    when RACE_ETHNCTY_CD = 1 then 8527--White|White
    when RACE_ETHNCTY_CD = 2 then 8516--Black|Black
    when RACE_ETHNCTY_CD = 3 then 8515--Asian|Asian
    when RACE_ETHNCTY_CD = 4 then 8657--AIAN|AIAN
    when RACE_ETHNCTY_CD = 5 then 8557--NAOPI|NAOPI
    when RACE_ETHNCTY_CD = 6 then 8522--|other
    when RACE_ETHNCTY_CD = 7 then 38003563--|Hispanic
    else 0--|Blank
  end as race_concept_id,
  case
    when ETHNCTY_CD = 0 then 38003564--|Not Hispanic
    when ETHNCTY_CD > 0 then 38003563--|Hispanic
    else 0
  end as ethnicity_concept_id,
--   null as location_id,
--  null as provider_id,
--  null as care_site_id,
  bene_id as person_source_value,
  SEX_CD as gender_source_value,
--  null as gender_source_concept_id,
  RACE_ETHNCTY_CD as race_source_value,
--  null as race_source_concept_id,
  ETHNCTY_CD as ethnicity_source_value--,
--  null as ethnicity_source_concept_id
 from
  demog_elig_base;

--drop table if exists dua_052538_nwi388.route_person;
--create table dua_052538_nwi388.route_person as 
insert into dua_052538_nwi388.route_person select 
  BENE_ID as person_id,
  gender_concept_id,
  year_of_birth,
  month_of_birth,
  day_of_birth,
  null as birth_datetime,
  race_concept_id,
  ethnicity_concept_id,
  null as location_id,
  null as provider_id,
  null as care_site_id,
  bene_id as person_source_value,
  gender_source_value,
  null as gender_source_concept_id,
  race_source_value,
  null as race_source_concept_id,
  ethnicity_source_value,
  null as ethnicity_source_concept_id
 from route_person;
  

-- COMMAND ----------

--table
--observation_period

--destination table: observation_period
--step number: 1
--step_name: hold

--this is needed for multi year

-- COMMAND ----------

--destination table: observation_period
--step number: 2
--step_name: transform

--this might be needed in multi year?

-- COMMAND ----------

--destination table: observation_period
--step number: 3
--step_name: route

--Fragment #1
--Fragment comment: extract observation period values for 05 destination

drop table if exists dua_052538_nwi388.route_observation_period;
create table dua_052538_nwi388.route_observation_period as
select
  ROW_NUMBER() OVER(
    ORDER BY
      bene_id ASC
  ) AS observation_period_id,
  bene_id as person_id,
  enrlmt_start_dt as observation_period_start_date,
  enrlmt_end_dt as observation_period_end_date,
  32817 as period_type_concept_id --|EHR no claims concept? 32810?
from
  demog_elig;


-- COMMAND ----------

--table
--death

--destination table: Death
--step number: 1
--step_name: hold

--multi year data will need to be extended here

-- COMMAND ----------

--destination table: Death
--step number: 2
--step_name: transform

--multi year data will need to be extended here

-- COMMAND ----------

--destination table: Death
--step number: 3
--step_name: route

--fragment # 1
--fragment comment: insert death values into table for 05 destination| null requires schema before insert into parquet or error

drop table if exists
dua_052538_nwi388.route_death;
create table  
dua_052538_nwi388.route_death 
( person_id bigint,
  death_date date,
  death_datetime string,
  death_type_concept_id bigint,
  cause_concept_id bigint,
  cause_source_value string,
  cause_source_concept_id bigint);


insert into dua_052538_nwi388.route_death 
select
  bene_id as person_id,
  DEATH_DT as death_date,
  null as death_datetime,
  32885 as death_type_concept_id,--|SSDI
  null as cause_concept_id,
  null as cause_source_value,
  null as cause_source_concept_id
from
  demog_elig_base
  where death_dt is not null
;

-- COMMAND ----------

--table
--provider

--hold_provider is broken in aws- need ccw to fix meta store
--hold_provider is now hold_provider1

--destination table: provider
--step number: 1
--step_name: hold

--fragment # 1
--fragment comment: create table to hold provider values
drop table if exists dua_052538_nwi388.hold_provider1;
create table dua_052538_nwi388.hold_provider1(
  forign_key string,
  complex_id string,
  npi string,
  specialty_source_value string,
  class string,
  origin_file string
);

--fragment #2
--fragment comment:load inpatient header rfrg class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32855) as forign_key,
  concat(state_cd, clm_id, "inpatient_header") as complex_id,
  RFRG_PRVDR_NPI as npi,
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "inpatient_header" as origin_file
from
  inpatient_header;
  
--fragment #3
  --fragment comment:load inpatient header ADMTG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32855) as forign_key,
  concat(state_cd, clm_id, "inpatient_header") as complex_id,
  ADMTG_PRVDR_NPI as npi,
  ADMTG_PRVDR_SPCLTY_CD as specialty_source_value,
  "ADMTG" as class,
  "inpatient_header" as origin_file
from
  inpatient_header;
  
--fragment #4
  --fragment comment:load inpatient header BLG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32855) as forign_key,
  concat(state_cd, clm_id, "inpatient_header") as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "inpatient_header" as origin_file
from
  inpatient_header;
  
--fragment #5
  --fragment comment:load inpatient line SRVC class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32855) as forign_key,
  concat(state_cd, clm_id, "inpatient_line") as complex_id,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "inpatient_line" as origin_file
from
  inpatient_line;
--fragment #6
  --fragment comment:load inpatient line OPRTG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32855) as forign_key,
  concat(state_cd, clm_id, "inpatient_line") as complex_id,
  OPRTG_PRVDR_NPI as npi,
  "NULL" as specialty_source_value,
  "OPRTG" as class,
  "inpatient_line" as origin_file
from
  inpatient_line;
--fragment #7
  --fragment comment:load other_services_header BLG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "other_services_header" as origin_file
from
  other_services_header;
--fragment #8
  --fragment comment: load other_services_header RFRG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  RFRG_PRVDR_NPI as npi,
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "other_services_header" as origin_file
from
  other_services_header;
--fragment #9
  --fragment comment:load other_services_header DRCTNG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  drctng_prvdr_npi as npi,
  "NULL" as specialty_source_value,
  "DRCTNG" as class,
  "other_services_header" as origin_file
from
  other_services_header;
--fragment #10
  --fragment comment:load other_services_header SPRVSNG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  sprvsng_prvdr_npi as npi,
  "NULL" as specialty_source_value,
  "SPRVSNG" as class,
  "other_services_header" as origin_file
from
  other_services_header;
--fragment #11
  --fragment comment:load other_services_header HLTH_HOME class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_header") as complex_id,
  hlth_home_prvdr_npi as npi,
  "NULL" as specialty_source_value,
  "HLTH_HOME" as class,
  "other_services_header" as origin_file
from
  other_services_header;
--fragment #12
  --fragment comment:load other_services_line HLTH_HOME class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32861) as forign_key,
  --outpatient claim header
  concat(state_cd, clm_id, "other_services_line") as complex_id,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "other_services_line" as origin_file
from
  other_services_line;
--fragment #13
  --fragment comment:load long_term_header RFRG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32846) as forign_key,
  --Facility claim header
  concat(state_cd, clm_id, "long_term_header") as complex_id,
  RFRG_PRVDR_NPI as npi,
  RFRG_PRVDR_SPCLTY_CD as specialty_source_value,
  "RFRG" as class,
  "long_term_header" as origin_file
from
  long_term_header;
--fragment #14
  --fragment comment:load long_term_header ADMTG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32846) as forign_key,
  --Facility claim header
  concat(state_cd, clm_id, "long_term_header") as complex_id,
  ADMTG_PRVDR_NPI as npi,
  ADMTG_PRVDR_SPCLTY_CD as specialty_source_value,
  "ADMTG" as class,
  "long_term_header" as origin_file
from
  long_term_header;
--fragment #15
  --fragment comment:load long_term_header BLG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32846) as forign_key,
  --Facility claim header
  concat(state_cd, clm_id, "long_term_header") as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "long_term_header" as origin_file
from
  long_term_header;
--fragment #16
  --fragment comment:load long_term_line SRVC class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32846) as forign_key,
  --Facility claim header
  concat(state_cd, clm_id, "long_term_line") as complex_id,
  SRVC_PRVDR_NPI as npi,
  SRVC_PRVDR_SPCLTY_CD as specialty_source_value,
  "SRVC" as class,
  "long_term_line" as origin_file
from
  long_term_line;
--fragment #17
  --fragment comment:load rx_header BLG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32869) as forign_key,
  --Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as complex_id,
  BLG_PRVDR_NPI as npi,
  BLG_PRVDR_SPCLTY_CD as specialty_source_value,
  "BLG" as class,
  "rx_header" as origin_file
from
  rx_header;
--fragment #18
  --fragment comment:load rx_header PRSCRBING class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32869) as forign_key,
  --Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as complex_id,
  PRSCRBNG_PRVDR_NPI as npi,
  "NULL" as specialty_source_value,
  "PRSCRBING" as class,
  "rx_header" as origin_file
from
  rx_header;
--fragment #19
  --fragment comment:load rx_header DSPNSNG class providers into hold
insert into
  dua_052538_nwi388.hold_provider1
select
  concat(clm_id, state_cd, 32869) as forign_key,
  --Pharmacy claim
  concat(state_cd, clm_id, "rx_header") as complex_id,
  DSPNSNG_PRVDR_NPI as npi,
  "NULL" as specialty_source_value,
  "DSPNSNG" as class,
  "rx_header" as origin_file
from
  rx_header;

-- COMMAND ----------

--destination table: provider
--step number: 2

--step_name: transform
--fragment #1
--fragment comment:make a list of distinct providers
drop view if exists transform_provider;
create view transform_provider as
select
  distinct npi
from
  dua_052538_nwi388.hold_provider1;
  
--fragment #2
  --fragment comment: fit provider details to distinct providers
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
  transform_provider a
  left join NPI_provider b on a.npi = b.npi;
  
--fragment #3
  --fragment comment:fit provider specialty to distinct providers
  drop table if exists dua_052538_nwi388.transform_provider_specialty;
create table dua_052538_nwi388.transform_provider_specialty using delta as
select
  distinct npi as npi_b,
  specialty_source_value
from
  dua_052538_nwi388.hold_provider1
where
  npi is not null
  and specialty_source_value is not null
  and specialty_source_value != 'NULL';
  
--fragment #4
  --fragment comment: load providers who are people and solve first-name last name as provider name
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
  
--fragment #5
  --fragment comment: load providers who are facility and solve organization name to provider name
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
  
--fragment # 6
  --fragment comment: union facility and indivdiual providers
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
  
--fragment # 7
  --fragment comment:generate provider id
  drop table if exists dua_052538_nwi388.provider_detail_all;
create table dua_052538_nwi388.provider_detail_all using delta as
select
  ROW_NUMBER() OVER(
    ORDER BY
      npi ASC
  ) as provider_id,
  provider_name,
  npi,
  provider_source_value
from
  provider_detail;

-- COMMAND ----------

--destination table: provider
--step number: 3
--step_name: route

--fragment #:1
--fragment comment: make ddl for provider records
drop table if exists dua_052538_nwi388.route_provider;
create table dua_052538_nwi388.route_provider(
  npi string,
  provider_id bigint,
  provider_name string,
  dea string,
  specialty_concept_id string,
  care_site_id string,
  year_of_birth string,
  gender_concept_id string,
  provider_source_value string,
  specialty_source_value string,
  specialty_source_concept_id string,
  gender_source_value string,
  gender_source_concept_id string
);


--fragment #:2
--fragment comment: inject providers into route provider
insert into
  dua_052538_nwi388.route_provider
select
  npi,
  provider_id,
  provider_name,
  null as dea,
  null as pecialty_concept_id,
  null as care_site_id,
  null as year_of_birth,
  null as gender_concept_id,
  null as provider_source_value,
  b.specialty_source_value,
  null as specialty_sourse_concept_id,
  null as gender_source_value,
  null as gender_source_concept_id
from
  dua_052538_nwi388.provider_detail_all a
  left join dua_052538_nwi388.transform_provider_specialty b on a.npi = b.npi_b;

-- COMMAND ----------

--hardcode

--destination table: visit_occurrence
--step number: 1
--step_name: hold



--fragment #1
--fragment comment: create holding table for visits
drop table if exists dua_052538_nwi388.hold_visit_occurrence;
create table dua_052538_nwi388.hold_visit_occurrence (
  bene_id bigint not null,
  visit_start_date date,
  visit_end_date date,
  visit_type_concept_id bigint,
  state_cd string,
  clm_id bigint,
  visit_source_value string,
  admitting_source_value string,
  visit_source_concept_id bigint,
  BLG_PRVDR_NPI string,
  admiting_source_concept_id string,
  forign_key string
);

--fragment #1
--fragment comment:load inpatient header visits into hold
insert into
  dua_052538_nwi388.hold_visit_occurrence
select
  bene_id,
  ADMSN_DT as visit_start_date,
  DSCHRG_DT as visit_end_date,
  32855 as visit_type_concept_id,  --inpatient claim header
  state_cd,
  clm_id,
  HOSP_TYPE_CD as visit_source_value,
  ADMSN_TYPE_CD as visit_source_value,
  case
    when HOSP_Type_CD = 00 then 42898160
    when HOSP_Type_CD = 01 then 4318944
    when HOSP_Type_CD = 02 then 4140387
    when HOSP_Type_CD = 03 then 37310591
    when HOSP_Type_CD = 04 then null
    when HOSP_Type_CD = 05 then 4268912
    when HOSP_Type_CD = 06 then null
    when HOSP_Type_CD = 07 then 4305507
    when HOSP_Type_CD = 08 then null
    else null
  end as  visit_source_concept_id,
  BLG_PRVDR_NPI,
  case
    when ADMSN_TYPE_CD = 1 then 4079617
    when ADMSN_TYPE_CD = 2 then 8782
    when ADMSN_TYPE_CD = 3 then 4314435
    when ADMSN_TYPE_CD = 4 then 45773140
    when ADMSN_TYPE_CD = 5 then 4079623
    else null
  end as admiting_source_concept_id,
  concat(clm_id, state_cd, 32855) as forign_key --inpatient claim header
from
  inpatient_header;

--fragment #2
--fragment comment:load long_term_care header visits into hold
insert into
  dua_052538_nwi388.hold_visit_occurrence
select
  bene_id,
  SRVC_BGN_DT as visit_start_date,
  srvc_end_dt as visit_end_date,
  32846 as visit_type_concept_id,  --Facility claim header
  state_cd,
  clm_id,
  null as visit_source_value,
  null as admitting_source_value,
  null as visit_source_concept_id,
  BLG_PRVDR_NPI,
  null as admiting_source_concept_id,
  concat(clm_id, state_cd, 32846) as forign_key --Facility claim header
from
  long_term_header;


  
--fragment #3
--fragment comment:create lkup cms place of service codes
drop view if exists lkup_cmspos;
create view lkup_cmspos as
select
  *
from
  dua_052538_nwi388.concept
where
  vocabulary_id = 'CMS Place of Service';

--fragment #4
--fragment comment:load other_services_header visits into hold
insert into
  dua_052538_nwi388.hold_visit_occurrence
select
  bene_id,
  srvc_bgn_dt as visit_start_date,
  srvc_end_dt as visit_end_date,
  32861 as visit_type_concept_id,  --outpatient claim header
  state_cd,
  clm_id,
  POS_CD as visit_source_value,
  null as admitting_source_value,
  b.concept_id as visit_source_concept_id,
case when a.BLG_PRVDR_NPI !='' then a.BLG_PRVDR_NPI
  else
  c.SRVC_PRVDR_NPI end as BLG_PRVDR_NPI,
  null as admiting_source_concept_id,
  concat(clm_id, state_cd, 32861) as forign_key--outpatient claim header
from
  other_services_header a
  left join lkup_cmspos b on a.POS_CD = b.concept_code
    left join other_services_line_p c on a.clm_id=c.clm_id_b;
  
--fragment #5
--fragment comment:load rx_header visits into hold

insert into
  dua_052538_nwi388.hold_visit_occurrence
select
  bene_id,
  RX_FILL_DT as visit_start_date,
  RX_FILL_DT as visit_end_date,
  581458 as visit_type_concept_id,
  state_cd,
  clm_id,
  null as visit_source_value,
  null as admitting_source_value,
  null as visit_source_concept_id,
  PRSCRBNG_PRVDR_NPI as BLG_PRVDR_NPI,
  null as admiting_source_concept_id,
  concat(clm_id, state_cd,581458 )as forign_key
from
  rx_header;

-- COMMAND ----------

--destination table: visit_occurrence
--step number: 2
--step_name: transform

--fragment #1
--fragment comment: create measures to evaluate visits
drop view if exists transform_visit_occurrence;
create view  transform_visit_occurrence as
select
  bene_id,
  visit_start_date,
  visit_end_date,
  BLG_PRVDR_NPI,
  visit_type_concept_id,
  concat(clm_id, state_cd, visit_type_concept_id) as forign_key,
  ROW_NUMBER() OVER(
    PARTITION BY bene_id
    ORDER BY
      visit_end_date,
      visit_start_date,
      clm_id ASC
  ) as event_id,
  (day(visit_end_date) - day(visit_start_date) + 1) as daydiff,
  rank(day(visit_end_date) - day(visit_start_date)) OVER (
    PARTITION BY bene_id
    ORDER BY
      visit_end_date asc
  ) as same_day,
  sum((day(visit_end_date) - day(visit_start_date) + 1)) over(
    partition by bene_id
    order by
      visit_end_date rows between unbounded preceding
      and current row
  ) as running_total -- 131 days when there should be 90
  --ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
from
  dua_052538_nwi388.hold_visit_occurrence
where
  visit_start_date is not null
order by
  bene_id,
  visit_start_date asc,
  visit_end_date desc,
  running_total desc;
  

-- COMMAND ----------

--destination table: visit_occurrence
--step number: 3
--step_name: route
--fragment #1
--fragment comment: create provider ids for visit occurrence table
drop view if exists provider_id_for_visit_occurrence;
create view provider_id_for_visit_occurrence as
select
  npi,
  provider_id
from
  dua_052538_nwi388.provider_detail_all;;
--fragment #2
  --fragment comment:create visit id/this is depreceated
  drop view if exists visit_id;
create view visit_id as
select
  distinct forign_key as visit_event,
  --complex visit rules can be set here
  visit_type_concept_id,
  forign_key,
  visit_start_date,
  visit_end_date,
  bene_id,
  event_id,
  BLG_PRVDR_NPI,
  same_day
from
  transform_visit_occurrence
group by
  visit_event,
  visit_type_concept_id,
  forign_key,
  visit_start_date,
  visit_end_date,
  bene_id,
  event_id,
  BLG_PRVDR_NPI,
  same_day;
--fragment #3
  --fragment comment:make visit id for visit occurrence
  drop view if exists visit_id_for_visit_occurrence;
create view visit_id_for_visit_occurrence as
select
  distinct forign_key,
  ROW_NUMBER() OVER (
    ORDER BY
      (
        forign_key --add same_day here for same day roll up
      )
  ) as visit_occurrence_id,
  visit_type_concept_id
from
  visit_id
group by
  forign_key,
  visit_type_concept_id;
--fragment #4
  --fragment comment:declare ddl to hold route_visit_occurrence
  drop table if exists dua_052538_nwi388.route_visit_occurrence;
create table dua_052538_nwi388.route_visit_occurrence(
    visit_occurrence_id bigint,
    person_id bigint,
    visit_concept_id bigint,
    visit_start_date date,
    visit_start_datetime string,
    visit_end_date date,
    visit_end_datetime string,
    visit_type_concept_id bigint,
    provider_id bigint,
    care_site_id bigint,
    visit_source_value string,
    visit_source_concept_id bigint,
    admitting_source_concept_id string,
    admitting_source_value string,
    discharge_to_concept_id bigint,
    discharge_to_source_value string,
    preceding_visit_occurrence_id bigint
  );
--fragment #5
  --fragment comment:populate route visit occurrence
insert into
  dua_052538_nwi388.route_visit_occurrence
select
  b.visit_occurrence_id,
  a.bene_id as person_id,
  null as visit_concept_id,
  a.visit_start_date,
  null as visit_start_datetime,
  a.visit_end_date,
  null as visit_end_datetime,
  a.visit_type_concept_id,
  c.provider_id,
  null as care_site,
  a.visit_source_value,
  a.visit_source_concept_id,
  a.admiting_source_concept_id,
  a.admitting_source_value,
  null as discharge_to_concept_id,
  null as discharge_to_source_value,
  null as preceding_visit_occurrence_id
from
  dua_052538_nwi388.hold_visit_occurrence a
  left join provider_id_for_visit_occurrence c on a.BLG_PRVDR_NPI = c.npi
  left join visit_id_for_visit_occurrence b on a.forign_key = b.forign_key;

-- COMMAND ----------

--table
--procedure_occurrence

--destination table: procedure_occurrence
--step number: 1
--step_name: hold


--fragment #1
--fragment comment: create hold for procedures
drop table if exists dua_052538_nwi388.hold_procedure_occurrence;
create table dua_052538_nwi388.hold_procedure_occurrence(
  bene_id bigint,
  clm_id bigint,
  origin_table string,
  origin string,
  procedure_type_concept_id bigint,
  modifier_concept_id string,
  event_source_value string,
  event_start_date date,
  complex_id string,
  npi string,
  vocabulary_id_b string);


--fragment #2
--fragment comment:extract  other_services_line procedures for hold
insert into
  dua_052538_nwi388.hold_procedure_occurrence
select
  bene_id,
  clm_id,
  "other_services_line" as origin_table,
  state_cd as origin,
  32861 as procedure_type_concept_id,
  null as modifier_concept_id,
  LINE_PRCDR_CD as event_source_value,
  LINE_PRCDR_CD_DT as event_start_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  srvc_prvdr_npi as npi,
  LINE_PRCDR_CD_SYS as vocabulary_id_b
from
  other_services_line
  where LINE_PRCDR_CD is not null
  and LINE_PRCDR_CD not like 'D%';--not dental procedures

--fragment #3
--fragment comment:extract inpatient header procedures for hold
insert into
  dua_052538_nwi388.hold_procedure_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  null as modifier_concept_id,
  prcdr_cd_1 as event_source_value,
  prcdr_cd_dt_1 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi,
  PRCDR_CD_SYS_1 as vocabulary_id_b
from
  inpatient_header
  where prcdr_cd_1 is not null;
  
  
--fragment #4
--fragment comment:extract inpatient header procedures for hold
insert into
  dua_052538_nwi388.hold_procedure_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  null as modifier_concept_id,
  prcdr_cd_2 as event_source_value,
  prcdr_cd_dt_2 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi,
  PRCDR_CD_SYS_2 as vocabulary_id_b
from
  inpatient_header
  where prcdr_cd_2 is not null;
  
  
--fragment #5
--fragment comment:extract inpatient header procedures for hold
insert into
  dua_052538_nwi388.hold_procedure_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  null as modifier_concept_id,
  prcdr_cd_3 as event_source_value,
  prcdr_cd_dt_3 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi,
  PRCDR_CD_SYS_3 as vocabulary_id_b
from
  inpatient_header
  where  prcdr_cd_3 is not null;
  
  
--fragment #6
--fragment comment:extract inpatient header procedures for hold
insert into
  dua_052538_nwi388.hold_procedure_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  null as modifier_concept_id,  
  prcdr_cd_4 as event_source_value,
  prcdr_cd_dt_4 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi,
  PRCDR_CD_SYS_4 as vocabulary_id_b
from
  inpatient_header
  where prcdr_cd_4 is not null;
  
  
--fragment #7
--fragment comment:extract inpatient header procedures for hold
insert into
  dua_052538_nwi388.hold_procedure_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
  null as modifier_concept_id,
prcdr_cd_5 as event_source_value,
  prcdr_cd_dt_5 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi,
  PRCDR_CD_SYS_5 as vocabulary_id_b
from
  inpatient_header
  where prcdr_cd_5 is not null;
  
  
--fragment #8
--fragment comment:extract inpatient header procedures for hold
insert into
  dua_052538_nwi388.hold_procedure_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  32855 as procedure_type_concept_id,
    null as modifier_concept_id,
  prcdr_cd_6 as event_source_value,
  prcdr_cd_dt_6 as event_start_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  blg_prvdr_npi as npi,
  PRCDR_CD_SYS_6 as vocabulary_id_b
from
  inpatient_header
  where prcdr_cd_6 is not null;

-- COMMAND ----------

--destination table:procedure_occurrence
--step number: 2
--step_name: transform

--fragment #1
--fragment comment: extract dental procedures from other_services _line
drop table if exists dua_052538_nwi388.hold_procedure_occurrence_d;
create table dua_052538_nwi388.hold_procedure_occurrence_d as
select
  bene_id,
  clm_id,
  "other_services_line" as origin_table,
  state_cd as origin,
  32861 as procedure_type_concept_id,
  LINE_PRCDR_CD as event_source_value,
  TOOTH_NUM,
  TOOTH_SRFC_CD,
  LINE_PRCDR_CD_DT as event_start_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  srvc_prvdr_npi as npi,
  LINE_PRCDR_CD_SYS as vocabulary_id_b
from
  other_services_line
where
  LINE_PRCDR_CD is not null
  and line_prcdr_cd like 'D%';
  
--fragment #2
  --fragment comment: recode dental as snomed codes
  drop view if exists transform_procedure_occurrence_d1;
create view transform_procedure_occurrence_d1 as
select
  bene_id,
  clm_id,
  origin_table,
  origin,
  procedure_type_concept_id,
  event_source_value,
  TOOTH_NUM,
  TOOTH_SRFC_CD,
  event_start_date,
  complex_id,
  npi,
  vocabulary_id_b,
  b.concept_id as CI_TN,
  b.concept_code as CC_TN,
  b.vocabulary_id as VI_TN,
  c.concept_id as CI_TS,
  c.concept_code as CC_TS,
  c.vocabulary_id as VI_TS,
  concat(d.concept_id_2, "_", e.concept_id_2) as mod_tooth
from
  dua_052538_nwi388.hold_procedure_occurrence_d a
  left join dua_052538_nwi388.concept b on a.tooth_num = b.concept_code
  and b.vocabulary_id = "CCW_Tooth_Number"
  left join dua_052538_nwi388.concept c on a.tooth_srfc_cd = c.concept_code
  and c.vocabulary_id = "CCW_Tooth_Surface"
  left join dua_052538_nwi388.concept_relationship d on b.concept_id = d.concept_id_1
  left join dua_052538_nwi388.concept_relationship e on c.concept_id = e.concept_id_1;
  
--fragment #3
  --fragent comment:inject dental into procedure hold
insert into
  dua_052538_nwi388.hold_procedure_occurrence
select
  bene_id,
  clm_id,
  origin_table,
  origin,
  procedure_type_concept_id,
  mod_tooth as modifier_concept_id,
  event_source_value,
  event_start_date,
  complex_id,
  npi,
  vocabulary_id_b
from
  transform_procedure_occurrence_d1;

-- COMMAND ----------

--destination table:procedure_occurrence
--step number: 3
--step_name: route


  
--fragment #1
--fragment comment:create provider ids for DX_PX_RX destination tables
drop view if exists provider_id_for_dxpxrx;
create view provider_id_for_dxpxrx as
select
 provider_id,
  npi as npi_a
from
   dua_052538_nwi388.route_provider;
  
--fragment #2
--fragment comment:create visit occurrence ids for DX_PX_RX destination tables
drop view if exists visit_id_for_dxpxrx;
create view visit_id_for_dxpxrx as
select
 visit_occurrence_id,
 forign_key
from
visit_id_for_visit_occurrence
;

--fragment #3
--fragment comment: make look up table for px codes
drop view if exists lkup_px;
create view lkup_px as
select
  concept_code,
  a.vocabulary_id,
  b.vocabulary_id_a,
  concept_id,
  concept_name,
  domain_id,
  standard_concept
from
  dua_052538_nwi388.concept a
inner join
dua_052538_nwi388.px_handshake b--from dictionary notebook
on
a.vocabulary_id=b.vocabulary_id
;


--fragment #4
--fragment comment:collect procedures for 05_destination
drop view if exists 
route_procedure_occurrence;


create view 
route_procedure_occurrence as
select
  *
from
  dua_052538_nwi388.hold_procedure_occurrence a
  left join visit_id_for_dxpxrx b on a.complex_id = b.forign_key
  
  left join lkup_px c on a.event_source_value = c.concept_code
  and vocabulary_id_b=vocabulary_id_a
 
  left join provider_id_for_dxpxrx d on a.npi = d.npi_a
where
  concept_id is not null;

-- COMMAND ----------

--fragment #5
--fragment comment:collect procedures route procedure
drop table if exists dua_052538_nwi388.route_PROCEDURE_OCCURRENCE;

create table dua_052538_nwi388.route_PROCEDURE_OCCURRENCE(
  procedure_occurrence_id bigint,
  person_id bigint,
  procedure_concept_id bigint,
  procedure_date date,
  procedure_datetime string,
  procedure_type_concept_id bigint,
  modifier_concept_id string,
  quantity bigint,
  provider_id string,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  procedure_source_value string,
  procedure_source_concept_id bigint,
  modifier_source_value string
);

--fragment #6
--fragment comment:populate procedures in route procedure
  insert into dua_052538_nwi388.route_procedure_occurrence
    select
  ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      clm_id,
      origin,
      origin_table ASC --consider using date to assign procedures first to last-
  ) as procedure_occurrence_id,
  bene_id as person_id,
  concept_id as procedure_concept_id,  --no work has been done to keep this value standard
  event_start_date as procedure_date,
  null as procedure_datetime,
  null as procedure_type_concept_id,  --requires ccw table
  modifier_concept_id, 
  null as quantity,
  provider_id as provider_id,
  visit_occurrence_id,
  visit_occurrence_id as visit_detail_id,
  event_source_value as procedure_source_value,
  concept_id as procedure_source_concept_id,
  null as modifier_source_value --not currently useing this
from route_procedure_occurrence
  ;

-- COMMAND ----------

--table
--condition_occurrence
--destination table: condition_occurrence

--step number: 1
--step_name: hold

--fragment #1
--fragment comment:make a table to hold conditons
drop table if exists dua_052538_nwi388.hold_condition_occurrence;
create table dua_052538_nwi388.hold_condition_occurrence (
  bene_id bigint,
  clm_id bigint,
  origin_table string,
  origin string,
  event_source_value string,
  event_start_date date,
  event_end_date date,
  complex_id string,
  npi string,
  condition_type_concept_id bigint
);

--fragment #2
--fragment comment:extract condtions from other_services_header
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  a.clm_id,
  "other_services_header" as origin_table,
  state_cd as origin,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  b.SRVC_PRVDR_NPI as npi,
  32861 as condition_type_concept_id
from
  other_services_header a
  left join other_services_line_p b on a.clm_id = b.clm_id_b;
  
--fragment #3
--fragment comment:extract condtions from other_services_header
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  a.clm_id,
  "other_services_header" as origin_table,
  state_cd as origin,
  DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  b.SRVC_PRVDR_NPI as npi,
  32861 as condition_type_concept_id
from
  other_services_header a
  left join other_services_line_p b on a.clm_id = b.clm_id_b;
  
--fragment #4
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  ADMTG_DGNS_CD as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #5
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #6
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #7
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_3 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #8
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_4 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #9
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_5 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #10
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_6 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #11
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_7 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #12
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_8 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #13
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_9 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #14
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_10 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #15
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_11 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #16
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  DGNS_CD_12 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #17
--fragment comment:extract dx from inpatient_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  drg_cd as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id
from
  inpatient_header;
  
--fragment #18
--fragment comment:extract dx from long_term_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
  
--fragment #19
--fragment comment:extract dx from long_term_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
  
--fragment #20
--fragment comment:extract dx from long_term_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_3 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
  
--fragment #21
--fragment comment:extract dx from long_term_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_4 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
  
--fragment #22
--fragment comment:extract dx from long_term_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  DGNS_CD_5 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;
  
--fragment #23
--fragment comment:extract dx from long_term_header for hold
insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  ADMTG_DGNS_CD as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id
from
  long_term_header;

-- COMMAND ----------

--destination table: condition_occurrence
--step number: 2
--step_name: transform


--deduplication should happen here


-- COMMAND ----------

--destination table: condition_occurrence
--step number: 3
--step_name: route

--fragment #1
--fragment comment:create look up for condition
drop view if exists lkup_ICD10CM;
--consider including icd9cm?
create view lkup_ICD10CM as
select
  concept_id,
  concept_name,
  domain_id,
  vocabulary_id,
  concept_class_id,
  standard_concept,
  REPLACE(concept_code, '.', '') as concept_code,
  valid_start_date,
  valid_end_date,
  invalid_reason
from
  dua_052538_nwi388.concept
where
  vocabulary_id = 'ICD10CM'
union
select
  *
from
  dua_052538_nwi388.concept
where
  vocabulary_id = 'DRG';
  
--fragment #2
  --fragment comment:collect conditions for 05_destination
  drop view if exists route_condition_occurrence;
create view route_condition_occurrence as
select
  d.provider_id,
  c.visit_occurrence_id,
  clm_id,
  origin_table,
  origin,
  event_source_value,
  event_start_date,
  event_end_date,
  complex_id,
  concept_name,
  concept_id,
  domain_id,
  vocabulary_id,
  bene_id,
  condition_type_concept_id
from
  dua_052538_nwi388.hold_condition_occurrence a
  left join lkup_ICD10CM b on a.event_source_value = b.concept_code
  left join visit_id_for_dxpxrx c on a.complex_id = c.forign_key
  left join provider_id_for_dxpxrx d on a.npi = d.npi_a;
  
--fragment #3
  --fragment comment: ddl to hold conditions
  drop table if exists dua_052538_nwi388.route_condition_occurrence;
create table dua_052538_nwi388.route_condition_occurrence(
    condition_occurrence_id bigint,
    person_id bigint,
    condition_concept_id bigint,
    condition_start_date date,
    condition_start_datetime string,
    condition_end_date date,
    condition_end_datetime string,
    condition_type_concept_id bigint,
    condition_status_concept_id bigint,
    stop_reason string,
    provider_id string,
    visit_occurrence_id bigint,
    visit_detail_id bigint,
    condition_source_value string,
    condition_source_concept_id bigint,
    condition_status_source_value string
  );
  
--fragment#4
  --fragment comment: load data into ddl
insert into
  dua_052538_nwi388.route_condition_occurrence
select
  ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      clm_id,
      origin,
      origin_table ASC --consider using date to assign procedures first to last-
  ) as condition_occurrence_id,
  bene_id as person_id,
  concept_id as condition_concept_id,
  event_start_date as condition_start_date,
  null as condition_start_datetime,
  event_end_date as condition_end_date,
  null as condition_end_datetime,
  condition_type_concept_id,
  --file type
  null condition_status_concept_id,
  --this could be solved with admt dx codes
  null as stop_reason,
  provider_id,
  visit_occurrence_id,
  null as visit_detail_id,
  event_source_value as condition_source_value,
  concept_id as condition_source_concept_id,
  null condition_status_source_value
from
  route_condition_occurrence
where
  domain_id = 'Condition';

-- COMMAND ----------

--table
--drug_exposure

--destination table:drug_exposure
--step number: 1
--step_name: hold

--fragment #1
--fragment comment: generate table to hold medications
drop table if exists dua_052538_nwi388.hold_drug_exposure;
create table dua_052538_nwi388.hold_drug_exposure (
  clm_id bigint,
  origin_table string,
  origin string,
  event_source_value string,
  dose_unit_source_value string,
  quantity integer,
  refills string,
  days_supply integer,
  route_source_value string,
  event_start_date date,
  event_end_date date,
  complex_id string,
  bene_id bigint,
  npi string
);

--fragment #2
--fragment comment: collect rx from other services line
insert into
  dua_052538_nwi388.hold_drug_exposure
select
  clm_id,
  "other_services_line" as origin_table,
  state_cd as origin,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32861) as complex_id,
  bene_id,
  srvc_prvdr_npi
from
  other_services_line
  where ndc is not null;;
  
--fragment #3
  --fragment comment:collect rx from inpatient line
insert into
  dua_052538_nwi388.hold_drug_exposure
select
  clm_id,
  "inpatient_line" as origin_table,
  state_cd as origin,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32855) as complex_id,
  bene_id,
  srvc_prvdr_npi
from
  inpatient_line
  where ndc is not null;;
  
--fragment #4
  --fragment comment:collect rx from rx header
insert into
  dua_052538_nwi388.hold_drug_exposure
select
  a.clm_id,
  "rx_line" as origin_table,
  state_cd as origin,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  NEW_RX_REFILL_NUM as refills,
  DAYS_SUPPLY as days_supply,
  DOSAGE_FORM_CD as route_source_value,
  RX_FILL_DT as event_start_date,
  date('01/01/0001') as event_end_date,
  concat(clm_id, state_cd, 32869) as complex_id,
  bene_id,
  b.PRSCRBNG_PRVDR_NPI
from
  rx_line a
  left join rx_header_p b on a.clm_id = b.clm_id_b
  where ndc is not null;;
  
--fragment #5
  --fragment comment: collect rx from long term line
insert into
  dua_052538_nwi388.hold_drug_exposure
select
  clm_id,
  "long_term_line" as origin_table,
  state_cd as origin,
  ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  0 as refills,
  0 as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(clm_id, state_cd, 32846) as complex_id,
  bene_id,
  SRVC_PRVDR_NPI
from
  long_term_line
  where ndc is not null;

-- COMMAND ----------

--destination table: drug_exposure
--step number: 2
--step_name: transform

--this is where ndc should become clinical drug?

-- COMMAND ----------

--destination table:drug_exposure
--step number: 3
--step_name: route

--fragment #1
--fragment comment:create look up table for ndc
drop view if exists lkup_NDC;
create view lkup_NDC as
select
  *
from
  dua_052538_nwi388.concept
where
  vocabulary_id = 'NDC';
  
--fragment #2
  --fragment comment:collect rx for 05_destination
  drop view if exists route_drug_exposure;
create view route_drug_exposure as
select
  *
from
  dua_052538_nwi388.hold_drug_exposure
  left join lkup_ndc on event_source_value = concept_code
  left join visit_id_for_dxpxrx on complex_id = forign_key
  left join provider_id_for_dxpxrx on npi = npi_a
where
  event_source_value is not null;
  
--fragment #3
  --fragment comment:create ddl for RX
  drop table if exists dua_052538_nwi388.route_DRUG_EXPOSURE;
create table dua_052538_nwi388.route_DRUG_EXPOSURE(
    drug_exposure_id bigint,
    person_id bigint,
    drug_concept_id bigint,
    drug_exposure_start_date date,
    drug_exposure_start_datetime string,
    drug_exposure_end_date date,
    drug_exposure_end_datetime string,
    verbatim_end_date date,
    drug_type_concept_id bigint,
    stop_reason string,
    refills string,
    quantity string,
    days_supply int,
    sig string,
    route_concept_id bigint,
    lot_number string,
    provider_id string,
    visit_occurrence_id bigint,
    visit_detail_id bigint,
    drug_source_value string,
    drug_source_concept_id bigint,
    route_source_value string,
    dose_unit_source_value string
  );
  
--fragment #4
  --fragment comment: load rx into route dx
insert into
  dua_052538_nwi388.route_DRUG_EXPOSURE
select
  ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      clm_id,
      origin,
      origin_table ASC --consider using date to assign procedures first to last-
  ) as drug_exposure_id,
  bene_id as person_id,
  concept_id as drug_concept_id,
  event_start_date as drug_exposure_start_date,
  null as drug_exposure_start_datetime,
  event_end_date as drug_exposure_end_date,
  null as drug_exposure_end_datetime,
  null as verbatim_end_date,
  32810 as drug_type_concept_id,
  null as stop_reason,
  refills,
  quantity,
  days_supply,
  null as sig,
  null as route_concept_id,
  null as lot_number,
  provider_id,
  visit_occurrence_id,
  null as visit_detail_id,
  concept_code as drug_source_value,
  concept_id as drug_source_concept_id,
  route_source_value,
  dose_unit_source_value
from
  route_drug_exposure;

-- COMMAND ----------

--destination table:observation
--step number: 3
--step_name: route

--fragment #1
--fragment comment: ddl for route
drop table if exists dua_052538_nwi388.route_observation;
create table dua_052538_nwi388.route_observation (
  observation_id bigint,
  person_id bigint,
  observation_concept_id bigint,
  observation_date date,
  observation_datetime string,
  observation_type_concept_id bigint,
  value_as_number string,
  value_as_string string,
  value_as_concept_id bigint,
  qualifier_concept_id bigint,
  unit_concept_id bigint,
  provider_id string,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  observation_source_value string,
  observation_source_concept_id bigint,
  unit_source_value string,
  qualifier_source_value string
);


--fragment #2 
--fragment comment: populate route

insert into dua_052538_nwi388.route_observation
select
ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      clm_id,
      origin,
      origin_table ASC --consider using date to assign procedures first to last-
  ) as obsercation_id,
bene_id as person_id,
concept_id as observation_concept_id,
event_start_date as observation_date,
null as observation_datetime,
32810 as observation_type_concept_id,
null as value_as_number,
null as value_as_string,
null as value_as_concept_id,
null as qualifier_concept_id,
null as unit_concept_id,
provider_id,
visit_occurrence_id,
null as visit_detail_id,
event_source_value as observation_source_value,
concept_id as observation_source_concept_id,
null as unit_source_value,
null as qualifier_source_value 
from route_condition_occurrence
where domain_id='Observation'

-- COMMAND ----------

 --destination table:observation
--step number: 3
--step_name: route

--fragment #1
--fragment comment: ddl for route
drop table if exists dua_052538_nwi388.route_MEASUREMENT;
create table dua_052538_nwi388.route_MEASUREMENT(
  measurement_id bigint,
  person_id bigint,
  measurement_concept_id bigint,
  measurement_date date,
  measurement_datetime string,
  measurement_time string,
  measurement_type_concept_id bigint,
  operator_concept_id bigint,
  value_as_number string,
  value_as_concept_id bigint,
  unit_concept_id bigint,
  range_low string,
  range_high string,
  provider_id string,
  visit_occurrence_id bigint,
  visit_detail_id bigint,
  measurement_source_value string,
  measurement_source_concept_id bigint,
  unit_source_value string,
  value_source_value string
);


--fragment #2 
--fragment comment: populate route
 
 insert into dua_052538_nwi388.route_measurement
 select
 ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      clm_id,
      origin,
      origin_table ASC --consider using date to assign procedures first to last-
  ) as measurement_id,
  bene_id as person_id,
  concept_id as measurement_concept_id,
  event_start_date as measurement_date,
  null as measurement_time,
  32810 as measurement_type_concept_id,
  null as operator_concept_id,
  null as value_as_number,
  null as value_as_concept_id,
  null as value_as_string,
  null as unit_concept_id,
  null as range_low,
  null as range_high,
  provider_id,
  visit_occurrence_id,
  null as visit_detail_id,
  event_source_value as measurement_source_value,
  concept_id as measurement_source_concept_id,
  null as unit_source_value,
  null as value_source_value
from
  route_condition_occurrence
where
  domain_id = 'Measurement'
