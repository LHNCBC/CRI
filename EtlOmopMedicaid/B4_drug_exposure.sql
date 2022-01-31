-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','drug exposure','1','start drug exposure start',current_timestamp(), null);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");

-- COMMAND ----------

--widget
create widget text job_id default "102";

-- COMMAND ----------


drop view if exists lkup_NDC;
create view lkup_NDC as
select
  *
from
  dua_052538_nwi388.concept
where
  vocabulary_id = 'NDC';

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','drug exposure','2','create lkup ndc',current_timestamp(), null);

-- COMMAND ----------

drop table if exists dua_052538_nwi388.hold_drug_exposure;
create table 
  dua_052538_nwi388.hold_drug_exposure using delta
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
  concat(clm_id, state_cd, 32861) as forign_key,--outpatient claim header
  bene_id,
  srvc_prvdr_npi,
  concept_id
from
dua_052538_nwi388.other_services_line a
left join lkup_ndc b 
  on a.ndc = b.concept_code
  where ndc is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','drug exposure','3','ot line to rx hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_drug_exposure
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
  --hardcode
  concat(clm_id, state_cd, 32855) as forign_key,--Inpatient Header Claim
  bene_id,
  srvc_prvdr_npi,
  concept_id
from
dua_052538_nwi388.inpatient_line a
left join lkup_ndc b 
  on a.ndc = b.concept_code
  where ndc is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','drug exposure','4','ip line to rx hold',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_drug_exposure
select
  a.clm_id,
  "rx_line" as origin_table,
  a.state_cd as origin,
  a.ndc as event_source_value,
  NDC_UOM_CD as dose_unit_source_value,
  NDC_QTY as quantity,
  NEW_RX_REFILL_NUM as refills,
  DAYS_SUPPLY as days_supply,
  DOSAGE_FORM_CD as route_source_value,
  RX_FILL_DT as event_start_date,
  date('01/01/0001') as event_end_date,
  concat(a.clm_id, a.state_cd, 32869) as forign_key,--Pharmacy claim
  a.bene_id,
  c.PRSCRBNG_PRVDR_NPI,
  b.concept_id
from
dua_052538_nwi388.rx_line a
left join lkup_ndc b 
  on a.ndc = b.concept_code
  left join 
 dua_052538_nwi388.rx_header c 
 on a.clm_id = c.clm_id
  where ndc is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','drug exposure','5','rx to rx hold',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_drug_exposure
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
  --hardcode
  concat(clm_id, state_cd, 32846) as forign_key,--Facility claim header
  bene_id,
  SRVC_PRVDR_NPI,
  concept_id
from
dua_052538_nwi388.long_term_line a
left join lkup_ndc b 
  on a.ndc = b.concept_code
  where ndc is not null;
  


-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','drug exposure','6','lt to rx hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.DRUG_EXPOSURE
select
  rand() as drug_exposure_id,
  bene_id as person_id,
  case  when concept_id='' then 0    --case where to set null ids to 0
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
   dua_052538_nwi388.hold_drug_exposure;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','drug exposure','6','rx to drug exposure cdm',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','drug exposure','7','rxend',current_timestamp(), null);