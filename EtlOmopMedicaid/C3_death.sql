-- Databricks notebook source

create widget text job_id default "102";


-- COMMAND ----------

--comment: insert values into route_death
insert into dua_052538_nwi388.route_death 
select
  bene_id as person_id,
  DEATH_DT as death_date,
  --hardcode
  0 as death_datetime,
  --hardcode
  32885 as death_type_concept_id,--|SSDI
  --hardcode
  0 as cause_concept_id,
  --hardcode
  0 as cause_source_value,
  --hardcode
  0 as cause_source_concept_id
from
 demog_elig_base
  where death_dt is not null;

-- COMMAND ----------

--comment: write route death as death table
insert into
  dua_052538_nwi388.death_$scope
select
  *
from
  dua_052538_nwi388.route_death;