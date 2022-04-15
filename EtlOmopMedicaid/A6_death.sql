-- Databricks notebook source

create widget text job_id default "102";


-- COMMAND ----------

--comment: write route death as death table
insert into
  dua_052538_nwi388.death
select
  bene_id as person_id,
  DEATH_DT as death_date,
  0 as death_datetime,
  32885 as death_type_concept_id,--|SSDI
  0 as cause_concept_id,
  0 as cause_source_value,
  0 as cause_source_concept_id
from
 dua_052538_nwi388.demog_elig_base
 where death_dt is not null;