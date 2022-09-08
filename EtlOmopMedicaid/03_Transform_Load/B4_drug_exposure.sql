-- Databricks notebook source
insert into <write_bucket>.log values('$job_id','drug exposure','1','start drug exposure start',current_timestamp() );

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");



-- COMMAND ----------


drop view if exists lkup_NDC;
create view lkup_NDC as
select
  *
from
  <write_bucket>.concept
where
  vocabulary_id = 'NDC';

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','drug exposure','2','create lkup ndc',current_timestamp() );

-- COMMAND ----------

drop table if exists <write_bucket>.hold_drug_exposure;

-- COMMAND ----------

create table 
  <write_bucket>.hold_drug_exposure using delta
select
  clm_id,
  "other_services_line" as origin_table,
  state_cd as origin,
  a.ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  '0' as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32861) as forign_key,--Outpatient claim header
  bene_id,
  srvc_prvdr_npi,
  concept_id
from
<write_bucket>.other_services_line_$year a
left join lkup_ndc b 
  on a.ndc = b.concept_code
  where ndc is not null;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','drug exposure','3','ot line to rx hold',current_timestamp() );

-- COMMAND ----------


insert into
  <write_bucket>.hold_drug_exposure
select
  clm_id,
  "inpatient_line" as origin_table,
  state_cd as origin,
  a.ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  '0' as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,--Inpatient Header Claim
  bene_id,
  srvc_prvdr_npi,
  concept_id
from
<write_bucket>.inpatient_line_$year a
left join lkup_ndc b 
  on a.ndc = b.concept_code
  where ndc is not null;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','drug exposure','4','ip line to rx hold',current_timestamp() );

-- COMMAND ----------

insert into
  <write_bucket>.hold_drug_exposure
select
 a.clm_id,
  "rx_line" as origin_table,
  a.state_cd as origin,
  a.ndc as event_source_value,
  a.NDC_UOM_CD as dose_unit_source_value,
  a.NDC_QTY as quantity,
  a.NEW_RX_REFILL_NUM as refills,
  a.DAYS_SUPPLY as days_supply,
  a.DOSAGE_FORM_CD as route_source_value,
  c.RX_FILL_DT as event_start_date,
  null as event_end_date,
  concat(a.clm_id,'_',a.state_cd,'_',a.year,'_',32869) as forign_key,--Pharmacy claim
  a.bene_id ,
  c.PRSCRBNG_PRVDR_NPI,
  b.concept_id
from
<write_bucket>.rx_line_$year a
left join lkup_ndc b 
  on a.ndc = b.concept_code
  left join 
 <write_bucket>.rx_header_$year c 
 on a.clm_id = c.clm_id
  where a.ndc is not null;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','drug exposure','5','rx to rx hold',current_timestamp() );

-- COMMAND ----------

insert into
  <write_bucket>.hold_drug_exposure
select
  clm_id,
  "long_term_line" as origin_table,
  state_cd as origin,
  a.ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  '0' as refills,
  '0' as days_supply,
  'NULL' as route_source_value,
  LINE_SRVC_BGN_DT as event_start_date,
  LINE_SRVC_END_DT as event_end_date,
  
  concat(clm_id,'_',state_cd,'_',year,'_', 32846) as forign_key,--Facility claim header
  bene_id,
  SRVC_PRVDR_NPI,
  concept_id
from
<write_bucket>.long_term_line_$year a
left join lkup_ndc b 
  on a.ndc = b.concept_code
  where ndc is not null;
  


-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','drug exposure','6','lt to rx hold',current_timestamp() );

-- COMMAND ----------


insert into
  <write_bucket>.DRUG_EXPOSURE
select
  null as drug_exposure_id,
  bene_id as person_id,
  case  when concept_id='' then 0  
  else concept_id end as drug_concept_id,
  event_start_date as drug_exposure_start_date,
  null as drug_exposure_start_datetime,
  event_end_date as drug_exposure_end_date,
  null as drug_exposure_end_datetime,
  null as verbatim_end_date,
    32810 as drug_type_concept_id,--EHR
  null as stop_reason,
  refills,
  quantity,
  days_supply,
  null as sig,
  null as route_concept_id,
  null as lot_number,
  srvc_prvdr_npi as provider_id,
  forign_key as visit_occurrence_id,
  null as visit_detail_id,
  event_source_value as drug_source_value,
  concept_id as drug_source_concept_id,
  route_source_value,
  dose_unit_source_value
from
   <write_bucket>.hold_drug_exposure;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','drug exposure','6','rx to drug exposure cdm',current_timestamp() );

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','drug exposure','7','rx end',current_timestamp() );