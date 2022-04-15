-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','condition occurrence','1','start condition occurrence',current_timestamp(), null);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #server terms
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions",7000);
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "True");

-- COMMAND ----------

create widget text job_id default "102";

-- COMMAND ----------

drop view if exists lkup_dx;
create view lkup_dx as
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
  vocabulary_id in( 'ICD10CM', 'ICD9CM')
union
select
  *
from
  dua_052538_nwi388.concept
where
  vocabulary_id = 'DRG';

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','2','create lkup dx',current_timestamp(), null);

-- COMMAND ----------


drop table if exists dua_052538_nwi388.hold_condition_occurrence;

create table 
dua_052538_nwi388.hold_condition_occurrence using delta as
select
  bene_id,
  clm_id,
  "other_services_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32861) as forign_key,
  BLG_PRVDR_NPI as npi,
  32861 as condition_type_concept_id,
  concept_id,
  domain_id 
from
 dua_052538_nwi388.other_services_Header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_1 = c.concept_code
where a.DGNS_CD_1 is not null
;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','3','ot header dx 1 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "other_services_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32861) as forign_key,
  BLG_PRVDR_NPI as npi,
  32861 as condition_type_concept_id,
  concept_id,
  domain_id 
from
 dua_052538_nwi388.other_services_Header_$year  a
left join lkup_dx c 
on 
a.DGNS_CD_2 = c.concept_code
where a.DGNS_CD_2 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','4','ot header dx 2 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.ADMTG_DGNS_CD as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year   a
left join lkup_dx c 
on 
a.ADMTG_DGNS_CD = c.concept_code
where a.ADMTG_DGNS_CD is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','5','ip header admit dx into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
 dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_1= c.concept_code
where a.DGNS_CD_1 is not null;



-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','6','ip header dx 1 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_2= c.concept_code
where a.DGNS_CD_2 is not null;


-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','7','ip header dx 2 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
 a.DGNS_CD_3 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_3= c.concept_code
where a.DGNS_CD_3 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','8','ip header dx 3 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_4 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
 32855 as condition_type_concept_id,
concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_4= c.concept_code
where a.DGNS_CD_4 is not null
;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','9','ip header dx 4 into condition hold',current_timestamp(), null);

-- COMMAND ----------


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
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_5= c.concept_code
where a.DGNS_CD_5 is not null
;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','10','ip header dx 5 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_6 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
    concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_6= c.concept_code
where a.DGNS_CD_6 is not null
;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','11','ip header dx 6 into condition hold',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_7 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_7= c.concept_code
where a.DGNS_CD_7 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','12','ip header dx 7 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_8 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_8= c.concept_code
where a.DGNS_CD_8 is not null


;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','13','ip header dx 8 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_9 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_', 32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
  
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_9= c.concept_code
where a.DGNS_CD_9 is not null


;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','14','ip header dx 9 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_10 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_10= c.concept_code
where a.DGNS_CD_10 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','15','ip header dx 10 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_11 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_11= c.concept_code
where a.DGNS_CD_11 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','16','ip header dx 11 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_12 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_12= c.concept_code 
where a.DGNS_CD_12 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','17','ip header dx 12 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "inpatient_header" as origin_table,
  state_cd as origin,
  a.drg_cd as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32855) as forign_key,
  BLG_PRVDR_NPI as npi,
  32855 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.inpatient_header_$year a
left join lkup_dx c 
on 
a.drg_cd= c.concept_code 
where a.drg_cd is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','18','ip header drg into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_1 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32846) asforign_key,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.long_term_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_1= c.concept_code 
where a.DGNS_CD_1 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','18','lt header dx 1 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_2 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32846) as forign_key,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id, 
  concept_id,
  domain_id 
from
dua_052538_nwi388.long_term_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_2= c.concept_code 
where a.DGNS_CD_2 is not null

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','19','lt header dx 2 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_3 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32846) as forign_key,
  BLG_PRVDR_NPI as npi,
   32846 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.long_term_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_3= c.concept_code 
where a.DGNS_CD_3 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','20','lt header dx 3 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_4 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32846) as forign_key,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.long_term_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_4= c.concept_code 
where a.DGNS_CD_4 is not null
;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','21','lt header dx 4 into condition hold',current_timestamp(), null);

-- COMMAND ----------


insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  a.DGNS_CD_5 as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',  32846) as forign_key,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id,
  concept_id,
  domain_id 
from
 dua_052538_nwi388.long_term_header_$year a
left join lkup_dx c 
on 
a.DGNS_CD_5= c.concept_code 
where a.DGNS_CD_5 is not null;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','22','lt header dx 5 into condition hold',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.hold_condition_occurrence
select
  bene_id,
  clm_id,
  "long_term_header" as origin_table,
  state_cd as origin,
  a.ADMTG_DGNS_CD as event_source_value,
  SRVC_BGN_DT as event_start_date,
  SRVC_END_DT as event_end_date,
  concat(clm_id,'_',state_cd,'_',year,'_',32846) as forign_key,
  BLG_PRVDR_NPI as npi,
  32846 as condition_type_concept_id,
  concept_id,
  domain_id 
from
dua_052538_nwi388.long_term_header_$year a
left join lkup_dx c 
on 
a.ADMTG_DGNS_CD= c.concept_code 
where a.ADMTG_DGNS_CD is not null;


-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','23','lt header admit dx into condition hold',current_timestamp(), null);

-- COMMAND ----------

insert into
  dua_052538_nwi388.condition_occurrence
select
  rand() as condition_occurrence_id,
  bene_id as person_id,
  case   when concept_id !='' then concept_id else 0  end as condition_concept_id,
  event_start_date as condition_start_date,
  null as condition_start_datetime,
  event_end_date as condition_end_date,
  null as condition_end_datetime,
  condition_type_concept_id,
  null condition_status_concept_id,
  null as stop_reason,
  npi as provider_id,
  forign_key as visit_occurrence_id,
  forign_key as visit_detail_id,
  event_source_value as condition_source_value,
  concept_id as condition_source_concept_id,
  null condition_status_source_value
from
    dua_052538_nwi388.hold_condition_occurrence
where
  domain_id = 'Condition';

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','24','conditon hold to condition occurrence cdm',current_timestamp(), null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','condition occurrence','25','end condition occurrence',current_timestamp(), null);