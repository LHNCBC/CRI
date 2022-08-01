


-- Databricks notebook source
insert into <write_bucket>.log values('$job_id','Death','1','start death',current_timestamp() );


-- COMMAND ----------


insert into
  <write_bucket>.death
select
  distinct bene_id as person_id,
  min(DEATH_DT) as death_date,
  0 as death_datetime,
  32885 as death_type_concept_id,--|SSDI
  0 as cause_concept_id,
  0 as cause_source_value,
  0 as cause_source_concept_id
from
 <write_bucket>.demog_elig_base
 where death_dt is not null 
 group by bene_id;

 
-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Death','2','end death',current_timestamp() );
