-- Databricks notebook source
insert into dua_052538_nwi388.log values('$job_id','Observation Period','1','start Observation Period',current_timestamp(),null);

-- COMMAND ----------

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
  --hardcode
  32817 as period_type_concept_id --|EHR no claims concept? 32810?
from
dua_052538_nwi388.demog_elig_dates;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Observation Period','2','create route Observation Period',current_timestamp(),null);

-- COMMAND ----------

insert into
  dua_052538_NWI388.OBSERVATION_PERIOD
select
  *
from
  dua_052538_nwi388.route_observation_period;

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Observation Period','3','route to Observation Period cdm',current_timestamp(),null);

-- COMMAND ----------

insert into dua_052538_nwi388.log values('$job_id','Observation Period','4','end Observation Period',current_timestamp(),null);