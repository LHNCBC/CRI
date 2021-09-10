-- Databricks notebook source
--

--


--

-- COMMAND ----------

create widget text job default "1s1m";

-- COMMAND ----------

--destination table: person
--step number: 
--step name: load

--fragment #1
--fragment comment:create person table from 04_etl
insert into
  dua_052538_NWI388.PERSON_$job
select
  *
from
  dua_052538_nwi388.route_person;

-- COMMAND ----------

--destination table: observation_period
--step number:
--step_name: load

--fragment #1
--fragment comment:create observation_period from 04_etl
insert into
  dua_052538_NWI388.OBSERVATION_PERIOD_$job
select
  *
from
  dua_052538_nwi388.route_observation_period;

-- COMMAND ----------

--destination table: Death
--step number: 
--step_name: load
--fragment #1
--fragment comment:create death table from 04_etl
insert into
  dua_052538_nwi388.death_$job
select
  *
from
  dua_052538_nwi388.route_death;

-- COMMAND ----------

--destination table:provider
--step number: 
--step_name: load
--fragment #1
--fragment comment:create provider table from 04_etl
insert into
  dua_052538_nwi388.provider_$job
select
  *
from
  dua_052538_nwi388.route_provider;

-- COMMAND ----------

--destination table: visit_occurrence
--step number: 
--step_name: load
--fragment #1
--fragment comment:create visit_occurrence from 04_etl
insert into
  dua_052538_nwi388.visit_occurrence_$job
select
  *
from
  dua_052538_nwi388.route_visit_occurrence;

-- COMMAND ----------

--destination table:procedure_occurrence
--step number: 
--step_name: load

--fragment #1
--fragment comment:create procedure_occurrence from 04_etl
insert into
  dua_052538_NWI388.procedure_occurrence_$job
select
  *
from
  dua_052538_nwi388.route_procedure_occurrence;

-- COMMAND ----------

--destination table: condition_occurrence
--step number: 
--step_name: load
--fragment #1
--fragment comment:create condition_occurrence from 04_etl
insert into
  dua_052538_nwi388.condition_occurrence_$job
select
  *
from
  dua_052538_nwi388.route_condition_occurrence;

-- COMMAND ----------

--destination table:
--step number: 
--step_name: load

--fragment #1
--fragment comment:create drug_exposure from 04_etl
insert into
  dua_052538_nwi388.DRUG_EXPOSURE_$job
select
 *
from
  dua_052538_nwi388.route_drug_exposure;

-- COMMAND ----------

--destination table:observation
--step number: 
--step_name: load


--fragment #1
--fragment comment:
insert into
  dua_052538_nwi388.observation_$job
select
  *
from
  dua_052538_nwi388.route_observation;

-- COMMAND ----------

--destination table: measurement
--step number:
--step_name: load

--fragment #
--fragment comment:
insert into
  dua_052538_nwi388.measurement_$job
select
  *
from
  dua_052538_nwi388.route_measurement;
