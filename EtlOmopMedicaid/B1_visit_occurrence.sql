-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','1','start visit occurrence',current_timestamp(), null);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");

-- COMMAND ----------

--widget
create widget text job_id default "102";

-- COMMAND ----------

drop table if exists dua_052538_nwi388.hold_visit_occurrence;

create table 
  dua_052538_nwi388.hold_visit_occurrence using delta as
select
  bene_id,
  SRVC_BGN_DT as visit_start_date,
  SRVC_end_DT as visit_end_date,
  32855 as visit_type_concept_id, 
  state_cd,
  clm_id,
  HOSP_TYPE_CD as visit_source_value,
  ADMSN_TYPE_CD as admitting_source_value,
  case
    when HOSP_Type_CD = 00 then 42898160 --not hospital|Non-hospital institution Visit
    when HOSP_Type_CD = 01 then 4318944--inpt hospital|Hospital
    when HOSP_Type_CD = 02 then 4140387--op hospital|Hospital Based OP Visit
    when HOSP_Type_CD = 03 then 37310591--CAH|Critical Access Hospital
    when HOSP_Type_CD = 04 then 0--swing bed hospital|unmaped/blank
    when HOSP_Type_CD = 05 then 4268912--inpt psy hosp|Psychiatric Hospital
    when HOSP_Type_CD = 06 then 0--IHS Hosptial|unmaped/blank
    when HOSP_Type_CD = 07 then 4305507--childrens hosptial|Childrens Hospital
    when HOSP_Type_CD = 08 then 0--other |unmaped/blank
    else 0
  end as  visit_source_concept_id,
  BLG_PRVDR_NPI,
  case
    when ADMSN_TYPE_CD = 1 then 4079617--emergency|Emergency Hosp Admit
    when ADMSN_TYPE_CD = 2 then 763902--urgent|Urgent Care Admit
    when ADMSN_TYPE_CD = 3 then 4314435--elective|Elective Admit
    when ADMSN_TYPE_CD = 4 then 765400--newborn|newborn
    when ADMSN_TYPE_CD = 5 then 4079623--trauma|Trauma Hospital Admit
    else 0
  end as admitting_source_concept_id,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key --inpatient claim header
from
dua_052538_nwi388.inpatient_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','create hold visit occurrence ip header','2','stat visit occurrence',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_visit_occurrence
select
  bene_id,
  SRVC_BGN_DT as visit_start_date,
  srvc_end_dt as visit_end_date,
  32846 as visit_type_concept_id,  
  state_cd,
  clm_id,
  null as visit_source_value,
  null as admitting_source_value,
  null as visit_source_concept_id,
  BLG_PRVDR_NPI,
  0 as admitting_source_concept_id,
  concat(clm_id,'_',state_cd,'_',year,'_', 32846) as forign_key
from
dua_052538_nwi388.long_term_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','3','lt header to hold visit occurrence',current_timestamp(), null);

-- COMMAND ----------

drop view if exists lkup_cmspos;
create view lkup_cmspos as
select
  *
from
dua_052538_nwi388.concept
where
  vocabulary_id = 'CMS Place of Service';

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','4','create lkup cms pos',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_visit_occurrence
select
  a.bene_id,
  a.srvc_bgn_dt as visit_start_date,
  a.srvc_end_dt as visit_end_date,
  32861 as visit_type_concept_id, 
  a.state_cd,
  a.clm_id,
  a.POS_CD as visit_source_value,
  null as admitting_source_value,
  b.concept_id as visit_source_concept_id,
  a.BLG_PRVDR_NPI as BLG_PRVDR_NPI,
  null as admitting_source_concept_id,
  concat(a.clm_id,'_',a.state_cd,'_',a.year,'_', 32861) as forign_key
from
dua_052538_nwi388.other_services_header_$year a
  left join 
 lkup_cmspos b on a.POS_CD = b.concept_code
    ;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','5','ot head to hold visit occurrence',current_timestamp(), null);

-- COMMAND ----------

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
  BLG_PRVDR_NPI as BLG_PRVDR_NPI,
  null as admitting_source_concept_id,
  concat(clm_id,'_',state_cd,'_',year,'_',581458) as forign_key
from
dua_052538_nwi388.rx_header_$year;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','6','rx head to visit occurrence hold',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','7','optimize visit occurrence hold',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.provider_id_for_visit_occurrence;
create table dua_052538_nwi388.provider_id_for_visit_occurrence as
select
  npi,
  provider_id
from
  dua_052538_nwi388.provider_detail_all;

optimize   dua_052538_nwi388.provider_id_for_visit_occurrence ZORDER by(npi);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','8','create provider id for visit occurrence',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.visit_occurrence 
select
  forign_key as visit_occurrence_id,
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
  a.admitting_source_concept_id,
  a.admitting_source_value,
  null as discharge_to_concept_id,
  null as discharge_to_source_value, 
  null as preceding_visit_occurrence_id 
from
  dua_052538_nwi388.hold_visit_occurrence a
  left join 
  dua_052538_nwi388.provider_id_for_visit_occurrence c 
  on a.BLG_PRVDR_NPI = c.npi
  ;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','9','hold to visit occurrence cdm',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Visit occurrence','10','end visit occurrence',current_timestamp(), null);