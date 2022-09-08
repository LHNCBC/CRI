-- Databricks notebook source
insert into <write_bucket>.log values('$job_id','Observation Period','1','start Observation Period',current_timestamp() );

-- COMMAND ----------

drop table if exists <write_bucket>.route_observation_period;
create table <write_bucket>.route_observation_period as
select
  ROW_NUMBER() OVER(
    ORDER BY
      bene_id,
      enrlmt_start_dt,
      enrlmt_end_dt ASC
  ) AS observation_period_id,
  bene_id as person_id,
  enrlmt_start_dt as observation_period_start_date,
  enrlmt_end_dt as observation_period_end_date,
  state_cd as period_type_concept_id 
from
<write_bucket>.demog_elig_dates;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Observation Period','2','create route Observation Period',current_timestamp() );

-- COMMAND ----------

insert into
  <write_bucket>.OBSERVATION_PERIOD
select
  *
from
  <write_bucket>.route_observation_period;

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Observation Period','3','route to Observation Period cdm',current_timestamp() );

-- COMMAND ----------

insert into <write_bucket>.log values('$job_id','Observation Period','4','end Observation Period',current_timestamp() );